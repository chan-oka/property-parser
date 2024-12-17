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
from datetime import datetime
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from vertexai.generative_models import GenerativeModel
from email.utils import parsedate_to_datetime

# Configuration
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
BUCKET_NAME = "property-parser-token"
TOKEN_FILE_NAME = "token.json"
EMAIL_ADDRESS = "daisaku.okada@okamolife.com"

# property_dataを作成する部分を修正
def format_date(date_str):
    try:
        # メールのタイムスタンプをパース
        parsed_date = parsedate_to_datetime(date_str)
        # BigQuery用のISO形式に変換
        return parsed_date.isoformat()
    except Exception as e:
        print(f"日付変換エラー: {e}")
        return datetime.now().isoformat()

def setup_gmail_service():
    """Gmail APIのセットアップ - トークンベースの認証を使用"""
    print("=== setup_gmail_service: 開始 ===")
    try:
        print("1. Storage Clientの初期化")
        storage_client = storage.Client()
        bucket = storage_client.bucket(BUCKET_NAME)
        token_blob = storage.Blob(TOKEN_FILE_NAME, bucket)

        print("2. 認証情報の確認開始")
        creds = None
        if token_blob.exists():
            print("3. トークンファイルが存在します")
            token_str = token_blob.download_as_string()
            token_json = json.loads(token_str)
            creds = Credentials.from_authorized_user_info(token_json, SCOPES)
            print("4. 認証情報を読み込みました")

        if not creds or not creds.valid:
            print("5. 認証情報の更新が必要です")
            if creds and creds.expired and creds.refresh_token:
                print("6. トークンをリフレッシュします")
                creds.refresh(Request())
                # 更新されたトークンを保存
                with open("/tmp/token.json", 'w') as token:
                    token.write(creds.to_json())
                token_blob.upload_from_filename(filename="/tmp/token.json")
                print("7. 新しいトークンを保存しました")
            else:
                print("8. エラー: 有効な認証情報がありません")
                raise Exception("Invalid credentials. Please re-authenticate.")

        print("9. Gmailサービスの構築開始")
        service = build('gmail', 'v1', credentials=creds)
        print("=== setup_gmail_service: 完了 ===")
        return service

    except Exception as e:
        print(f"ERROR in setup_gmail_service: {str(e)}")
        raise

def setup_services():
    """全サービスの初期化"""
    print("=== setup_services: 開始 ===")
    try:
        # Gemini APIのセットアップ
        print("1. Gemini APIの初期化")

        vertexai.init(project=os.environ['PROJECT_ID'], location='us-central1')

        system_instruction = "あなたは優秀なエグゼクティブアシスタントです。毎日大量に届くメールから不動産の物件情報を正確に抽出・整理することを得意としています。"
        model = GenerativeModel(
            model_name="gemini-1.5-flash-001",
            system_instruction=[
                system_instruction
            ])

        # Gmail APIのセットアップ
        print("2. Gmail APIの初期化")
        gmail_service = setup_gmail_service()

        # BigQueryクライアントの初期化
        print("3. BigQuery Clientの初期化")
        bq_client = bigquery.Client()

        print("=== setup_services: 完了 ===")
        return gmail_service, model, bq_client
    except Exception as e:
        print(f"ERROR in setup_services: {str(e)}")
        raise

def get_unread_emails(service):
    """未読メールの取得と解析"""
    print("=== get_unread_emails: 開始 ===")
    try:
        print("1. 未読メールの検索開始")
        results = service.users().messages().list(
            userId='me',
            q='is:unread',
            maxResults=10
        ).execute()

        if not results.get('messages'):
            print("2. 未読メールはありません")
            return []

        messages = results.get('messages', [])[:10]  # ここで10件に制限
        print(f"3. 未読メール数: {len(messages)} (10件に制限)")

        print(f"3. 未読メール数: {len(results.get('messages', []))}")
        emails = []
        for index, message in enumerate(results['messages']):
            print(f"\n--- メール {index + 1} の処理開始 ---")
            try:
                msg = service.users().messages().get(
                    userId='me',
                    id=message['id'],
                    format='full'
                ).execute()

                print(f"4. メールID {message['id']} の内容を取得")
                payload = msg['payload']
                headers = payload['headers']

                # ヘッダー情報の取得
                subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
                from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
                date = next((h['value'] for h in headers if h['name'].lower() == 'date'), '')

                print(f"5. ヘッダー情報: Subject={subject}, From={from_email}")

                # 本文の取得とデコード
                body = ''
                if 'parts' in payload:
                    print("6. マルチパートメールの処理")
                    for part in payload['parts']:
                        if part.get('mimeType') == 'text/plain' and 'data' in part.get('body', {}):
                            encoded_data = part['body']['data']
                            encoded_data = encoded_data.replace("-", "+").replace("_", "/")
                            body = base64.b64decode(encoded_data).decode('utf-8', 'ignore')
                            print("7. プレーンテキスト部分を取得")
                            break
                elif 'body' in payload and 'data' in payload['body']:
                    print("8. シンプルメールの処理")
                    encoded_data = payload['body']['data']
                    encoded_data = encoded_data.replace("-", "+").replace("_", "/")
                    body = base64.b64decode(encoded_data).decode('utf-8', 'ignore')

                if not body:
                    print("9. 本文が空のためスキップ")
                    continue

                if not ('不動産' in body or '物件' in body):
                    print("9.1 不動産関連のキーワードが含まれていないためスキップ")
                    continue

                emails.append({
                    'id': message['id'],
                    'subject': subject,
                    'from': from_email,
                    'date': date,
                    'content': body
                })
                print(f"10. メール {index + 1} の処理完了")

            except Exception as e:
                print(f"ERROR processing email {message['id']}: {str(e)}")
                continue

        print(f"\n=== get_unread_emails: 完了 (処理メール数: {len(emails)}) ===")
        return emails

    except Exception as e:
        print(f"ERROR in get_unread_emails: {str(e)}")
        return []

def mark_as_read(service, message_id):
    """メールを既読にマーク"""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        print(f"メールID {message_id} を既読にマークしました")
    except Exception as e:
        print(f"Error marking message {message_id} as read: {str(e)}")
        raise

def analyze_email_with_gemini(model, email_content, email_subject):
    """メール内容をGeminiで分析"""
    print("=== analyze_email_with_gemini: 開始 ===")
    try:
        time.sleep(1)

        prompt = f"""
        以下の不動産物件情報メールから、必要な情報を抽出してJSON形式で返してください。
        未記載の項目はnullとしてください。

        数値データと日付に関する重要な規則：
        - price（価格）: 数値のみで返してください。例："560万円" → 5600000
        - yield_rate（利回り）: 数値のみで返してください。例："15.00%" → 15.00
        - construction_date（建築年月日）: 必ずYYYY-MM-DD形式で返してください。
          * 年月のみの場合（YYYY-MM）は、01日として YYYY-MM-01 の形式で返してください
          * 例1: "1979-10" → "1979-10-01"
          * 例2: "1979" → "1979-01-01"
        - その他の数値フィールドも単位や記号は付けず、純粋な数値のみで返してください
        - 数値は全て小数点以下2桁までとしてください

        メールタイトル：
        {email_subject}

        メール本文：
        {email_content}

        JSON:
        "property_name": "物件名",
        "property_type": "物件種別",
        "postal_code": "郵便番号",
        "prefecture": "都道府県",
        "city": "市区町村",
        "address": "番地以降の住所",
        "price": 価格（数値のみ。例：5600000）,
        "monthly_fee": 月額費用（数値のみ）,
        "management_fee": 管理費（数値のみ）,
        "floor_area": 専有面積（数値のみ）,
        "floor_number": 階数（数値のみ）,
        "total_floors": 総階数（数値のみ）,
        "nearest_station": "最寄駅",
        "station_distance": 駅までの距離（数値のみ）,
        "building_age": 築年数（数値のみ）,
        "construction_date": "建築年月日（必ずYYYY-MM-DD形式。例：1979-10-01）",
        "features": ["設備1", "設備2"],
        "status": "募集中",
        "source_company": "情報提供会社",
        "company_phone": "電話番号",
        "company_email": "メールアドレス",
        "property_url": "URL",
        "road_price": 路線価（数値のみ）,
        "estimated_price": 積算価格（数値のみ）,
        "current_rent_income": 現況家賃収入（数値のみ）,
        "expected_rent_income": 想定家賃収入（数値のみ）,
        "yield_rate": 利回り（数値のみ。例：15.00）,
        "land_area": 敷地面積（数値のみ）
        """

        print("1. Geminiに分析リクエスト送信")
        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        print("2. Geminiから応答を受信")
        print(f"応答内容: {response.text}")

        try:
            return json.loads(response.text)
        except json.JSONDecodeError as e:
            print(f"JSON パースエラー: {e}")
            print(f"受信した応答: {response.text}")
            # JSON パースに失敗した場合はエラー情報を含むデフォルト値を返す
            return {
                "error": "解析エラー",
                "raw_response": response.text[:500],  # 応答の先頭500文字を保存
            }
    except Exception as e:
        print(f"Error in analyze_email_with_gemini: {str(e)}")
        raise

@functions_framework.http
def process_property_emails(request):
    """メインハンドラー"""
    print("\n=== process_property_emails: 開始 ===")
    try:
        print("1. 各サービスの初期化開始")
        gmail_service, model, bq_client = setup_services()

        print("2. 未読メールの取得開始")
        emails = get_unread_emails(gmail_service)
        if not emails:
            print("3. 未読メールはありません")
            return {'status': 'success', 'message': 'No unread emails found'}

        processed_count = 0
        print(f"4. メール処理開始 (総数: {len(emails)})")

        for email_data in emails:
            try:
                print(f"\n--- メール {email_data['id']} の分析開始 ---")
                property_data = analyze_email_with_gemini(
                    model,
                    email_data['content'],
                    email_data['subject']
                )

                # 必須フィールドの追加
                current_time = datetime.now().isoformat()
                property_data.update({
                    'id': str(uuid.uuid4()),
                    'email_id': email_data['id'],
                    'email_subject': email_data['subject'],
                    'email_body': email_data['content'],
                    'email_received_at': format_date(email_data['date']),
                    'email_from': email_data['from'],
                    'created_at': current_time,
                    'updated_at': current_time
                })

                # BigQueryに保存
                print("5. BigQueryにデータを保存")
                table_id = f"{os.environ['PROJECT_ID']}.property_data.properties"
                errors = bq_client.insert_rows_json(table_id, [property_data])

                if errors:
                    raise Exception(f"BigQuery insertion errors: {errors}")

                # print("6. メールを既読にマーク")
                # mark_as_read(gmail_service, email_data['id'])
                processed_count += 1

            except Exception as e:
                print(f"""
                === ERROR: メール {email_data['id']} の処理中にエラー発生 ===
                メールID: {email_data['id']}
                エラータイプ: {type(e).__name__}
                エラーメッセージ: {str(e)}
                メール本文: {email_data['content']}
                発生時刻: {datetime.now().isoformat()}
                ===============
                """)
                continue

        print(f"\n=== process_property_emails: 完了 (処理完了: {processed_count}/{len(emails)}) ===")
        return {
            'status': 'success',
            'processed_emails': processed_count,
            'total_emails': len(emails)
        }

    except Exception as e:
        print(f"CRITICAL ERROR in process_property_emails: {str(e)}")
        return {'status': 'error', 'message': str(e)}, 500
