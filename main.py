import functions_framework
import google.cloud.storage as storage
import google.cloud.bigquery as bigquery
import google.generativeai as genai
import json
import os
import uuid
import base64
import time
import vertexai
import logging
from datetime import datetime
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from vertexai.generative_models import GenerativeModel
from email.utils import parsedate_to_datetime
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from json.decoder import JSONDecodeError

# ロギングの設定
logger = logging.getLogger('property_processor')
handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Configuration
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
BUCKET_NAME = os.environ['BUCKET_NAME']
TOKEN_FILE_NAME = os.environ['TOKEN_FILE_NAME']
EMAIL_ADDRESS = os.environ['EMAIL_ADDRESS']

def setup_gmail_service():
    """Gmail APIのセットアップ - トークンベースの認証を使用"""
    logger.info("Gmail APIのセットアップを開始")
    try:
        logger.debug("Storage Clientの初期化")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        token_blob = storage.Blob(TOKEN_FILE_NAME, bucket)

        logger.debug("認証情報の確認開始")
        creds = None
        if token_blob.exists():
            logger.debug("トークンファイルが存在します")
            token_str = token_blob.download_as_string()
            token_json = json.loads(token_str)
            creds = Credentials.from_authorized_user_info(token_json, SCOPES)
            logger.debug("認証情報を読み込みました")

        if not creds or not creds.valid:
            logger.info("認証情報の更新が必要です")
            if creds and creds.expired and creds.refresh_token:
                logger.info("トークンをリフレッシュします")
                creds.refresh(Request())
                with open("/tmp/token.json", 'w') as token:
                    token.write(creds.to_json())
                token_blob.upload_from_filename(filename="/tmp/token.json")
                logger.info("新しいトークンを保存しました")
            else:
                logger.error("有効な認証情報がありません")
                raise Exception("Invalid credentials. Please re-authenticate.")

        logger.debug("Gmailサービスの構築開始")
        service = build('gmail', 'v1', credentials=creds)
        logger.info("Gmail APIのセットアップが完了しました")
        return service

    except Exception as e:
        logger.error(f"Gmail APIセットアップ中にエラーが発生: {str(e)}", exc_info=True)
        raise

def setup_services():
    """全サービスの初期化"""
    logger.info("サービスの初期化を開始")
    try:
        # Gemini APIのセットアップ
        logger.debug("Gemini APIの初期化")
        vertexai.init(project=os.environ['PROJECT_ID'], location='us-central1')

        system_instruction = "あなたは優秀なエグゼクティブアシスタントです。..."
        model = GenerativeModel(
            model_name="gemini-1.5-flash-001",
            system_instruction=[system_instruction]
        )

        # Gmail APIのセットアップ
        logger.debug("Gmail APIの初期化")
        gmail_service = setup_gmail_service()

        # BigQueryクライアントの初期化
        logger.debug("BigQuery Clientの初期化")
        bq_client = bigquery.Client()

        logger.info("全サービスの初期化が完了しました")
        return gmail_service, model, bq_client
    except Exception as e:
        logger.error(f"サービス初期化中にエラーが発生: {str(e)}", exc_info=True)
        raise

def analyze_email_with_gemini(model, email_content, email_subject):
    """メール内容をGeminiで分析"""
    logger.info("Geminiでのメール分析を開始")
    try:
        time.sleep(3)  # APIレート制限対策

        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        try:
            result = json.loads(response.text)
        except json.JSONDecodeError as e:
            logger.error(f"Gemini応答のJSONパースに失敗: {e}")
            logger.debug(f"受信した応答: {response.text[:500]}")
            return {
                "error": "解析エラー",
                "raw_response": response.text[:500]
            }

        if not isinstance(result, list):
            logger.warning(f"""
            Geminiからの応答が配列形式ではありません
            応答タイプ: {type(result)}
            応答内容: {result}
            """)
            raise ValueError("Gemini response must be an array")

        logger.info(f"Gemini分析成功: {len(result)}件の物件情報を抽出")
        return result

    except Exception as e:
        logger.error(f"Gemini分析中にエラーが発生: {str(e)}", exc_info=True)
        raise e

def save_to_bigquery(bq_client, property_data_list):
    """BigQueryにデータを保存"""
    try:
        if not property_data_list:
            logger.warning("保存するデータがありません")
            return False

        table_id = f"{os.environ['PROJECT_ID']}.property_data.properties"
        errors = bq_client.insert_rows_json(table_id, property_data_list)
        if errors:
            logger.error(f"BigQueryデータ挿入エラー: {errors}")
            return False

        logger.info(f"{len(property_data_list)}件の物件データをBigQueryに保存しました")
        return True
    except Exception as e:
        logger.error(f"BigQueryへの保存中にエラーが発生: {str(e)}", exc_info=True)
        return False

def process_property_email(message_data, service, model, bq_client):
    """個別のメールを処理"""
    try:
        logger.info(f"メールID {message_data['id']} の処理を開始")

        # メール本文の取得
        msg = service.users().messages().get(
            userId='me',
            id=message_data['id'],
            format='full'
        ).execute()

        # ヘッダー情報の抽出と検証
        subject, from_email, date = extract_email_headers(msg['payload']['headers'])
        logger.debug(f"メールヘッダー: Subject='{subject}', From='{from_email}'")

        # メール処理のメインロジック
        body = decode_email_body(msg['payload'])
        if not body:
            logger.info(f"メールID {message_data['id']}: 本文が空のためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        if not ('不動産' in body or '物件' in body):
            logger.info(f"メールID {message_data['id']}: 不動産関連キーワードなしのためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        # Geminiでの分析とデータ処理
        property_data_list = analyze_email_with_gemini(model, body, subject)
        if not property_data_list:
            logger.info(f"メールID {message_data['id']}: Gemini分析結果が空のためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        logger.debug(f"Gemini分析結果: {json.dumps(property_data_list, indent=2)}")

        # データの保存とメールの既読化
        if save_to_bigquery(bq_client, processed_properties):
            logger.info(f"メールID {message_data['id']}: 処理完了")
            mark_as_read(service, message_data['id'])
            return {
                'id': message_data['id'],
                'subject': subject,
                'from': from_email,
                'date': date,
                'properties': processed_properties
            }
        else:
            logger.error(f"メールID {message_data['id']}: BigQuery保存に失敗")
            return None

    except Exception as e:
        logger.error(f"""
        メールID {message_data['id']} の処理中にエラーが発生
        エラータイプ: {type(e).__name__}
        エラーメッセージ: {str(e)}
        """, exc_info=True)
        return None

@functions_framework.http
def process_property_emails(request):
    """メインハンドラー"""
    logger.info("不動産メール処理を開始")
    try:
        # サービスの初期化
        gmail_service, model, bq_client = setup_services()

        # メールの処理
        emails, processed_count = process_unread_property_emails(gmail_service, model, bq_client)

        if not emails:
            logger.info("処理完了: 処理対象のメールはありませんでした")
            return {
                'status': 'success',
                'message': 'No emails processed',
                'processed_emails': 0,
                'total_emails': 0
            }

        logger.info(f"処理完了: {processed_count}件のメールを正常に処理")
        return {
            'status': 'success',
            'message': f'Successfully processed {processed_count} emails',
            'processed_emails': processed_count,
            'total_emails': len(emails)
        }

    except Exception as e:
        logger.critical(f"メール処理中に重大なエラーが発生: {str(e)}", exc_info=True)
        return {
            'status': 'error',
            'message': str(e),
            'processed_emails': 0,
            'total_emails': 0
        }, 500