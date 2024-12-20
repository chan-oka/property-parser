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
from tenacity import retry, stop_after_attempt, wait_fixed, retry_if_exception_type
from json.decoder import JSONDecodeError

# Configuration
SCOPES = ["https://www.googleapis.com/auth/gmail.modify"]
BUCKET_NAME = os.environ['BUCKET_NAME']
TOKEN_FILE_NAME = os.environ['TOKEN_FILE_NAME']
EMAIL_ADDRESS = os.environ['EMAIL_ADDRESS']


def format_date(date_str):
    """メールのタイムスタンプをBigQuery用のISO形式に変換"""
    try:
        parsed_date = parsedate_to_datetime(date_str)
        return parsed_date.isoformat()
    except Exception as e:
        print(f"日付変換エラー: {e}")
        return datetime.now().isoformat()


def convert_to_yen(value):
    """
    万円単位の数値を円単位の整数値に変換する
    例：
        21.8 (21.8万円) -> 218,000円
        0.5 (0.5万円) -> 5,000円
    """
    try:
        if value is None:
            return None

        # 文字列の場合は数値に変換
        if isinstance(value, str):
            # カンマを除去して数値変換
            value = value.replace(',', '')
            value = float(value)

        # float型の値を円単位で整数に変換（万円 → 円）
        return int(value * 10000)

    except (ValueError, TypeError) as e:
        print(f"Warning: Failed to convert value to yen. Value: {value}, Error: {e}")
        return None

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
            system_instruction=[system_instruction]
        )

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


def extract_email_headers(headers):
    """メールヘッダーから必要な情報を抽出"""
    try:
        subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
        from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
        date = next((h['value'] for h in headers if h['name'].lower() == 'date'), '')
        return subject, from_email, date
    except Exception as e:
        print(f"Error extracting headers: {e}")
        return 'No Subject', '', datetime.now().isoformat()


def decode_email_body(payload):
    print(f"""
    === Email Structure ===
    MIME Type: {payload.get('mimeType')}
    Has Parts: {'parts' in payload}
    Parts Count: {len(payload.get('parts', []))}
    ==================
    """)

    """
    メール本文をデコード - multipart/mixedを含む様々なMIMEタイプに対応
    """

    def find_message_parts_text(message, message_parts=None):
        if message_parts is None:
            message_parts = {"text/plain": None, "text/html": None}

        mimetype = message.get("mimeType", "")

        # multipart形式の処理
        if mimetype.startswith("multipart/"):
            for part in message.get("parts", []):
                find_message_parts_text(part, message_parts)
            return message_parts

        # text形式の処理
        if mimetype == "text/plain" and message.get("body", {}).get("data"):
            try:
                data = message["body"]["data"]
                text = base64.urlsafe_b64decode(data).decode("utf-8")
                message_parts["text/plain"] = text
            except Exception as e:
                print(f"Error decoding text/plain: {e}")

        elif mimetype == "text/html" and message.get("body", {}).get("data"):
            try:
                data = message["body"]["data"]
                text = base64.urlsafe_b64decode(data).decode("utf-8")
                message_parts["text/html"] = text
            except Exception as e:
                print(f"Error decoding text/html: {e}")

        # 他のパートがある場合は再帰的に処理
        for part in message.get("parts", []):
            find_message_parts_text(part, message_parts)

        return message_parts

    try:
        print(f"Processing email with MIME type: {payload.get('mimeType', 'unknown')}")
        message_parts = find_message_parts_text(payload)

        # text/plainを優先
        if message_parts["text/plain"]:
            return message_parts["text/plain"]

        # text/plainがない場合はHTMLから抽出
        if message_parts["text/html"]:
            try:
                from bs4 import BeautifulSoup
                soup = BeautifulSoup(message_parts["text/html"], 'html.parser')
                return soup.get_text(separator=' ', strip=True)
            except Exception as e:
                print(f"Error parsing HTML: {e}")

        print("No text content found in email")
        return ''

    except Exception as e:
        print(f"Error decoding email body: {str(e)}")
        print(f"Payload structure: {json.dumps(payload, indent=2)[:500]}")
        return ''

@retry(
    stop=stop_after_attempt(3),  # 最大3回試行
    wait=wait_fixed(3),  # 3秒間隔で待機
    retry=retry_if_exception_type((Exception)),  # リトライする例外の種類
    before_sleep=lambda retry_state: print(f"リトライ {retry_state.attempt_number}/3 を {3}秒後に実行します...")
)
def analyze_email_with_gemini(model, email_content, email_subject):
    """メール内容をGeminiで分析"""
    print("=== analyze_email_with_gemini: 開始 ===")
    try:
        time.sleep(3)  # APIレート制限対策

        prompt = f"""
        以下の不動産物件情報メールから、必要な情報を抽出してJSON形式で返してください。

        重要な注意事項：
        - 未記載の項目はnullとしてください。
        - メール内に複数の物件情報がある場合は、配列形式で全ての物件情報を返してください。
        - 必ず配列形式で返してください。物件情報が1件の物件の場合でも配列として返してください。

        数値データと日付に関する重要な規則：
        - price（物件価格）: 数値のみで返してください。例："560万円" → 5600000
        - yield_rate（利回り）: 数値のみで返してください。例："15.00%" → 15.00
        - construction_date（建築年月日）: 必ずYYYY-MM-DD形式で返してください。
          * 年月のみの場合（YYYY-MM）は、01日として YYYY-MM-01 の形式で返してください
          * 例1: "1979-10" → "1979-10-01"
          * 例2: "1979" → "1979-01-01"
        - その他の数値フィールドも単位や記号は付けず、純粋な数値のみで返してください
        - 数値は全て小数点以下2桁までとしてください
        - features（設備）: 設備情報は全て配列に含めてください。数の制限はありません。
        - station_distance: 必ず徒歩での所要時間を分単位の整数で返してください。
          * 例1: "徒歩15分" → 15
          * 例2: "徒歩5分" → 5
          * 距離（km/m）が与えられた場合は、80m/分として計算してください
          * 例3: "3.3km" → 41（3300m ÷ 80m/分 ≈ 41分）
  
        メールタイトル：
        {email_subject}

        メール本文：
        {email_content}

        JSON:
        "property_name": "物件名"
        "property_type": "物件種別"
        "postal_code": "郵便番号"
        "prefecture": "都道府県"
        "city": "市区町村"
        "address": "番地以降の住所"
        "price": 物件価格（数値のみ。例：5600000）
        "monthly_fee": 月額費用（数値のみ）
        "management_fee": 管理費（数値のみ）
        "floor_area": 専有面積（数値のみ）
        "floor_number": 階数（数値のみ）
        "total_floors": 総階数（数値のみ）
        "railway_line": 路線名（例：「JR山手線 渋谷駅」の場合 → JR山手線）
        "station_name": 駅名（例：「JR山手線 渋谷駅」の場合 → 渋谷駅）
        "station_distance": 駅までの徒歩距離（分単位の整数値のみ。例：徒歩15分 → 15）
        "building_age": 築年数（数値のみ）
        "construction_date": "建築年月日（必ずYYYY-MM-DD形式。例：1979-10-01）"
        "features": ["設備1", "設備2", ...]
        "status": "募集中"
        "source_company": "情報提供会社"
        "company_phone": "電話番号"
        "company_email": "メールアドレス"
        "property_url": "URL"
        "road_price": 路線価（数値のみ）
        "estimated_price": 積算価格（数値のみ）
        "current_rent_income": 現況家賃収入（数値のみ）
        "expected_rent_income": 想定家賃収入（数値のみ）
        "yield_rate": 利回り（数値のみ。例：15.00）
        "land_area": 敷地面積（数値のみ）
        """

        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        try:
            result = json.loads(response.text)
        except json.JSONDecodeError as e:
            print(f"JSON パースエラー: {e}")
            print(f"受信した応答: {response.text}")
            return {
                "error": "解析エラー",
                "raw_response": response.text[:500]
            }

        # 配列形式でない場合は例外として処理
        if not isinstance(result, list):
            print(f"""
            === WARNING: Geminiからの応答が配列形式ではありません ===
            応答タイプ: {type(result)}
            応答内容: {result}
            ===============
            """)
            raise ValueError("Gemini response must be an array")

        print(f"分析成功: {len(result)}件の物件情報を抽出")
        return result

    except Exception as e:
        print(f"Error in analyze_email_with_gemini: {str(e)}")
        raise e


def prepare_property_data(property_data, email_info):
    """プロパティデータに必要なフィールドを追加"""
    try:
        current_time = datetime.now().isoformat()
        property_data.update({
            'id': str(uuid.uuid4()),
            'email_id': email_info['id'],
            'email_subject': email_info['subject'],
            'email_body': email_info['body'],
            'email_received_at': format_date(email_info['date']),
            'email_from': email_info['from'],
            'created_at': current_time,
            'updated_at': current_time
        })
        return property_data
    except Exception as e:
        print(f"Error preparing property data: {e}")
        return None


def save_to_bigquery(bq_client, property_data_list):
    """BigQueryにデータを保存"""
    try:
        if not property_data_list:
            print("保存するデータがありません")
            return False

        # BigQuery保存用にデータを変換
        converted_properties = []
        for property_data in property_data_list:
            converted_data = property_data.copy()

            # 万円単位のフィールドを円単位に変換
            for field in ['road_price', 'expected_rent_income']:
                if field in converted_data:
                    converted_data[field] = convert_to_yen(converted_data[field])

            converted_properties.append(converted_data)

        table_id = f"{os.environ['PROJECT_ID']}.property_data.properties"
        errors = bq_client.insert_rows_json(table_id, converted_properties)
        if errors:
            print(f"BigQuery insertion errors: {errors}")
            return False

        print(f"{len(converted_properties)}件の物件データをBigQueryに保存しました")
        return True
    except Exception as e:
        print(f"Error saving to BigQuery: {e}")
        return False


def mark_as_read(service, message_id):
    """メールを既読にマーク"""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        print(f"メールID {message_id} を既読にマークしました")
        return True
    except Exception as e:
        print(f"Error marking message {message_id} as read: {str(e)}")
        return False


def filter_valid_properties(property_data_list):
    """有効な物件データのみをフィルタリング"""
    valid_properties = []
    skipped_properties = []

    for property_data in property_data_list:
        if property_data.get('price') is not None:
            valid_properties.append(property_data)
        else:
            skipped_properties.append(property_data.get('property_name', '名称不明'))

    if skipped_properties:
        print(
            f"以下の物件は price が未設定のためスキップされました: {', '.join(str(name) for name in skipped_properties)}")

    return valid_properties

def process_property_email(message_data, service, model, bq_client):
    """個別のメールを処理"""
    try:
        print(f"\n--- メールID {message_data['id']} の処理開始 ---")

        # メール本文の取得
        msg = service.users().messages().get(
            userId='me',
            id=message_data['id'],
            format='full'
        ).execute()

        # ヘッダー情報の抽出
        subject, from_email, date = extract_email_headers(msg['payload']['headers'])
        print(f"ヘッダー情報: Subject={subject}, From={from_email}")

        # 本文のデコード
        body = decode_email_body(msg['payload'])
        if not body:
            # メールを既読にマーク
            if not mark_as_read(service, message_data['id']):
                print("既読マークに失敗")
            print("本文が空のためスキップ")
            return None

        # 不動産関連チェック
        if not ('不動産' in body or '物件' in body):
            # メールを既読にマーク
            if not mark_as_read(service, message_data['id']):
                print("既読マークに失敗")
            print("不動産関連のキーワードが含まれていないためスキップ")
            return None

        # メール情報の整理
        email_info = {
            'id': message_data['id'],
            'subject': subject,
            'from': from_email,
            'date': date,
            'body': body
        }

        # Geminiでの分析
        print("Geminiでの分析開始")
        print(f"subject: {subject}")
        print(f"body: {body}")

        property_data_list = analyze_email_with_gemini(model, body, subject)
        if not property_data_list:
            # メールを既読にマーク
            if not mark_as_read(service, message_data['id']):
                print("既読マークに失敗")
            print("Geminiでの分析結果が空のためスキップ")
            return None

        print(f"Geminiでの分析結果: {property_data_list}")

        filtered_properties = filter_valid_properties(property_data_list)
        if not filtered_properties:
            # メールを既読にマーク
            if not mark_as_read(service, message_data['id']):
                print("既読マークに失敗")
            print("処理可能な物件データがありません")
            return None

        # データの準備
        processed_properties = []
        for property_data in filtered_properties:
            extended_data = prepare_property_data(property_data, email_info)
            if extended_data:
                processed_properties.append(extended_data)
            else:
                print("物件データの準備に失敗")

        if not processed_properties:
            # メールを既読にマーク
            if not mark_as_read(service, message_data['id']):
                print("既読マークに失敗")
            print("処理可能な物件データがありません")
            return None

        # BigQueryへの保存
        print("BigQueryへの保存開始")
        if not save_to_bigquery(bq_client, processed_properties):
            print("BigQueryへの保存に失敗")
            return None

        # メールを既読にマーク
        if not mark_as_read(service, message_data['id']):
            print("既読マークに失敗")
            # 保存は完了しているのでエラーとはしない

        print(f"メールID {message_data['id']} の処理が完了")
        return {**email_info, 'properties': processed_properties}

    except Exception as e:
        print(f"""
        === ERROR: メール {message_data['id']} の処理中にエラー発生 ===
        メールID: {message_data['id']}
        エラータイプ: {type(e).__name__}
        エラーメッセージ: {str(e)}
        発生時刻: {datetime.now().isoformat()}
        ===============
        """)
        return None


def process_unread_property_emails(service, model, bq_client):
    """未読の不動産関連メールを処理"""
    print("=== process_unread_property_emails: 開始 ===")
    processed_count = 0

    try:
        # 未読メールの検索
        results = service.users().messages().list(
            userId='me',
            q='is:unread label:不動産',
            maxResults=100
        ).execute()

        if not results.get('messages'):
            print("未読メールはありません")
            return [], 0

        messages = results.get('messages', [])
        print(f"未読メール数: {len(messages)}")

        processed_emails = []
        for index, message in enumerate(messages):
            print(f"\n--- メール {index + 1}/{len(messages)} の処理開始 ---")

            result = process_property_email(message, service, model, bq_client)
            if result:
                processed_emails.append(result)
                processed_count += 1
                print(f"メール {index + 1} の処理が完了しました")
            else:
                print(f"メール {index + 1} の処理がスキップまたは失敗しました")

        print(f"\n=== process_unread_property_emails: 完了 (処理完了: {processed_count}/{len(messages)}) ===")
        return processed_emails, processed_count

    except Exception as e:
        print(f"ERROR in process_unread_property_emails: {str(e)}")
        return [], 0


@functions_framework.http
def process_property_emails(request):
    """メインハンドラー"""
    print("\n=== process_property_emails: 開始 ===")
    try:
        # サービスの初期化
        gmail_service, model, bq_client = setup_services()

        # メールの処理
        emails, processed_count = process_unread_property_emails(gmail_service, model, bq_client)

        if not emails:
            print("処理完了: メールは処理されませんでした")
            return {
                'status': 'success',
                'message': 'No emails processed',
                'processed_emails': 0,
                'total_emails': 0
            }

        print(f"処理完了: {processed_count}件のメールが正常に処理されました")
        return {
            'status': 'success',
            'message': f'Successfully processed {processed_count} emails',
            'processed_emails': processed_count,
            'total_emails': len(emails)
        }

    except Exception as e:
        error_message = f"CRITICAL ERROR in process_property_emails: {str(e)}"
        print(error_message)
        return {
            'status': 'error',
            'message': error_message,
            'processed_emails': 0,
            'total_emails': 0
        }, 500