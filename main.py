import functions_framework
import google.cloud.storage as storage
import google.cloud.bigquery as bigquery
import json
import os
import uuid
import base64
from datetime import datetime
from bs4 import BeautifulSoup
from google.auth.transport.requests import Request
from google.oauth2.credentials import Credentials
from googleapiclient.discovery import build
from googleapiclient.errors import HttpError

# Configuration
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
BUCKET_NAME = "property-parser-token"
TOKEN_FILE_NAME = "token.json"
EMAIL_ADDRESS = "daisaku.okada@okamolife.com"


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


def get_unread_emails(service):
    """未読メールの取得と解析"""
    print("=== get_unread_emails: 開始 ===")
    try:
        print("1. 未読メールの検索開始")
        results = service.users().messages().list(
            userId='me',
            q='is:unread'
        ).execute()

        if not results.get('messages'):
            print("2. 未読メールはありません")
            return []

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

                print("10. メールを既読にマーク")
                service.users().messages().modify(
                    userId='me',
                    id=message['id'],
                    body={'removeLabelIds': ['UNREAD']}
                ).execute()

                emails.append({
                    'id': message['id'],
                    'subject': subject,
                    'from': from_email,
                    'date': date,
                    'content': body
                })
                print(f"11. メール {index + 1} の処理完了")

            except Exception as e:
                print(f"ERROR processing email {message['id']}: {str(e)}")
                continue

        print(f"\n=== get_unread_emails: 完了 (処理メール数: {len(emails)}) ===")
        return emails

    except Exception as e:
        print(f"ERROR in get_unread_emails: {str(e)}")
        return []


@functions_framework.http
def process_property_emails(request):
    """メインハンドラー"""
    print("\n=== process_property_emails: 開始 ===")
    try:
        print("1. Gmailサービスの初期化開始")
        gmail_service = setup_gmail_service()

        print("4. 未読メールの取得開始")
        emails = get_unread_emails(gmail_service)
        if not emails:
            print("5. 未読メールはありません")
            return {'status': 'success', 'message': 'No unread emails found'}

        processed_count = len(emails)
        print(f"6. 処理完了 (処理メール数: {processed_count})")

        return {
            'status': 'success',
            'processed_emails': processed_count,
            'message': f'Successfully processed {processed_count} emails'
        }

    except Exception as e:
        print(f"ERROR in process_property_emails: {str(e)}")
        return {'status': 'error', 'message': str(e)}, 500
