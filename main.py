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
from datetime import datetime, timezone, timedelta
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


def get_jst_now():
    """現在時刻をJST(UTC+9)で取得"""
    JST = timezone(timedelta(hours=+9), 'JST')
    return datetime.now(JST).isoformat()

def format_date(date_str):
    """メールのタイムスタンプをBigQuery用のISO形式に変換（JST）"""
    try:
        # メールのタイムスタンプをパース
        parsed_date = parsedate_to_datetime(date_str)
        # JSTに変換
        JST = timezone(timedelta(hours=+9), 'JST')
        jst_date = parsed_date.astimezone(JST)
        return jst_date.isoformat()
    except Exception as e:
        logger.error(f"日付変換エラー: {e}")
        return get_jst_now()


def convert_to_yen(value):
    """万円単位の数値を円単位の整数値に変換"""
    try:
        if value is None:
            return 0

        if isinstance(value, str):
            if not value.strip():
                return 0

            value = value.replace(',', '')
            value = float(value)

        return int(value * 10000)

    except (ValueError, TypeError) as e:
        logger.warning(f"円単位変換失敗: Value: {value}, Error: {e}")
        return None


def convert_floor_to_int(floor_str):
    """
    階数文字列を整数に変換
    例：
    - '1F', '1階', '1' → 1
    - 'B1F', 'B1', '地下1階', '地下1' → -1
    - '15F', '15階' → 15
    - 'B2F', 'B2', '地下2階' → -2
    """
    if not floor_str:
        return None

    try:
        if isinstance(floor_str, int):
            return floor_str

        floor_str = str(floor_str).upper().strip()

        # 数字のみの場合
        if floor_str.isdigit():
            return int(floor_str)

        # 地下階の場合
        if 'B' in floor_str or '地下' in floor_str:
            number = int(''.join(filter(str.isdigit, floor_str)))
            return -number

        # 地上階の場合
        return int(''.join(filter(str.isdigit, floor_str)))

    except Exception as e:
        print(f"Floor number conversion error for: {floor_str} - Error: {str(e)}")
        return None


def convert_japanese_era_date(date_str):
    """
    和暦の場合のみ西暦に変換。それ以外はそのまま返す。
    """
    if not date_str or not isinstance(date_str, str):
        return date_str

    try:
        date_str = date_str.upper().strip()

        # 和暦の場合のみ変換
        if any(era in date_str for era in ['R', 'H', 'S']):
            number = int(''.join(filter(str.isdigit, date_str)))
            if 'R' in date_str:  # 令和
                year = 2018 + number
            elif 'H' in date_str:  # 平成
                year = 1988 + number
            elif 'S' in date_str:  # 昭和
                year = 1925 + number
            return f"{year}-01-01"

        # 和暦以外はそのまま返す
        return date_str

    except Exception as e:
        print(f"Date conversion error for: {date_str} - Error: {str(e)}")
        return date_str


def convert_building_age(age_str):
    """
    築年数を整数に変換
    例：
    - '3.9' → 3
    - '10.5' → 10
    - '5' → 5
    """
    if not age_str:
        return None

    try:
        if isinstance(age_str, (int, float)):
            return int(age_str)

        # 文字列から数値を抽出して整数化
        age_str = str(age_str).strip()
        return int(float(age_str))

    except Exception as e:
        print(f"Building age conversion error for: {age_str} - Error: {str(e)}")
        return None


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

        system_instruction = "あなたは優秀なエグゼクティブアシスタントです。毎日大量に届くメールから不動産の物件情報を正確に抽出・整理することを得意としています。"
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


def extract_email_headers(headers):
    """メールヘッダーから必要な情報を抽出"""
    try:
        subject = next((h['value'] for h in headers if h['name'].lower() == 'subject'), 'No Subject')
        from_email = next((h['value'] for h in headers if h['name'].lower() == 'from'), '')
        date = next((h['value'] for h in headers if h['name'].lower() == 'date'), '')
        return subject, from_email, date
    except Exception as e:
        logger.error(f"ヘッダー抽出エラー: {e}", exc_info=True)
        return 'No Subject', '', get_jst_now()


def decode_email_body(payload):
    """メール本文をデコード"""
    logger.debug(f"""
    メール構造:
    MIME Type: {payload.get('mimeType')}
    Has Parts: {'parts' in payload}
    Parts Count: {len(payload.get('parts', []))}
    """)

    def find_message_parts_text(message, message_parts=None):
        if message_parts is None:
            message_parts = {"text/plain": None, "text/html": None}

        mimetype = message.get("mimeType", "")

        if mimetype.startswith("multipart/"):
            for part in message.get("parts", []):
                find_message_parts_text(part, message_parts)
            return message_parts

        if mimetype == "text/plain" and message.get("body", {}).get("data"):
            try:
                data = message["body"]["data"]
                text = base64.urlsafe_b64decode(data).decode("utf-8")
                message_parts["text/plain"] = text
            except Exception as e:
                logger.error(f"text/plain デコードエラー: {e}", exc_info=True)

        elif mimetype == "text/html" and message.get("body", {}).get("data"):
            try:
                data = message["body"]["data"]
                text = base64.urlsafe_b64decode(data).decode("utf-8")
                message_parts["text/html"] = text
            except Exception as e:
                logger.error(f"text/html デコードエラー: {e}", exc_info=True)

        for part in message.get("parts", []):
            find_message_parts_text(part, message_parts)

        return message_parts

    try:
        logger.debug(f"MIME type: {payload.get('mimeType', 'unknown')}")
        message_parts = find_message_parts_text(payload)

        if message_parts["text/plain"]:
            return message_parts["text/plain"]

        if message_parts["text/html"]:
            try:
                soup = BeautifulSoup(message_parts["text/html"], 'html.parser')
                return soup.get_text(separator=' ', strip=True)
            except Exception as e:
                logger.error(f"HTML解析エラー: {e}", exc_info=True)

        logger.warning("メール本文が見つかりません")
        return ''

    except Exception as e:
        logger.error(f"メール本文デコードエラー: {str(e)}", exc_info=True)
        logger.debug(f"Payload structure: {json.dumps(payload, indent=2)[:500]}")
        return ''

@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(3),
    retry=retry_if_exception_type((Exception)),
    before_sleep=lambda retry_state: logger.info(f"リトライ {retry_state.attempt_number}/3 を {3}秒後に実行します...")
)
def analyze_email_with_gemini(model, email_content, email_subject):
    """メール内容をGeminiで分析"""
    logger.info("Geminiでのメール分析を開始")
    try:
        time.sleep(3)  # APIレート制限対策

        prompt = f"""
        以下の不動産物件情報メールから、必要な情報を抽出してJSON形式で返してください。

        重要な注意事項：
        - 未記載の項目はnullとしてください。
        - メール内に複数の物件情報がある場合は、配列形式で全ての物件情報を返してください。
        - 必ず配列形式で返してください。物件情報が1件の物件の場合でも配列として返してください。

        数値データと日付に関する重要な規則：
        - price（物件価格）: 必ず円単位で返してください。単位込みの例：
          * "1,580万円" → 15800000
          * "5,280万円" → 52800000
          * "13,700万円" → 137000000
          * "2億3,800万円" → 238000000
        - station_name（駅名）: 末尾の「駅」は除いて返してください。例：
          * "東武練馬駅" → "東武練馬"
          * "東十条駅" → "東十条"
        - railway_line（路線名）: 正式な路線名を返してください。例：
          * 東武練馬駅の場合 → "東武東上線"
          * 東十条駅の場合 → "JR京浜東北線"
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
        "price": 物件価格（数値のみ）
        "monthly_fee": 月額費用（数値のみ）
        "management_fee": 管理費（数値のみ）
        "floor_area": 専有面積（数値のみ）
        "floor_number": 階数（数値のみ）
        "total_floors": 総階数（数値のみ）
        "railway_line": "路線名"
        "station_name": "駅名"
        "station_distance": 駅までの徒歩時間（分）
        "building_age": 築年数（数値のみ）
        "construction_date": "建築年月日（YYYY-MM-DD形式）"
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
        "yield_rate": 利回り（数値のみ）
        "land_area": 敷地面積（数値のみ）
        """

        response = model.generate_content(prompt, generation_config={"response_mime_type": "application/json"})
        try:
            result = json.loads(response.text)
        except json.JSONDecodeError as e:
            logger.error(f"Gemini応答のJSONパースに失敗: {e}")
            logger.info(f"受信した応答: {response.text}")
            raise e

        if not isinstance(result, list):
            logger.warning(f"""
            Geminiからの応答が配列形式ではありません
            応答タイプ: {type(result)}
            応答内容: {result}
            """)
            raise ValueError("Gemini response must be an array")

        logger.info(f"Gemini分析成功: {len(result)}件の物件情報を抽出")
        return result

    except json.JSONDecodeError as e:
        return []
    except Exception as e:
        logger.error(f"Gemini分析中にエラーが発生: {str(e)}", exc_info=True)
        raise e

def prepare_property_data(property_data, email_info):
    """プロパティデータに必要なフィールドを追加"""
    try:
        current_time = get_jst_now()
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
        logger.error(f"プロパティデータ準備中にエラーが発生: {e}", exc_info=True)
        return None

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
        logger.warning(f"価格未設定のためスキップされた物件: {', '.join(str(name) for name in skipped_properties)}")

    return valid_properties

def save_to_bigquery(bq_client, property_data_list):
    """BigQueryにデータを保存"""
    try:
        if not property_data_list:
            logger.warning("保存するデータがありません")
            return False

        # BigQuery保存用にデータを変換
        converted_properties = []
        for property_data in property_data_list:
            converted_data = property_data.copy()

            # 万円単位のフィールドを円単位に変換
            for field in ['road_price', 'current_rent_income', 'expected_rent_income']:
                if field in converted_data:
                    converted_data[field] = convert_to_yen(converted_data[field])

            # 階数文字列を整数に変換
            for field in ['floor_number']:
                if field in converted_data:
                    converted_data[field] = convert_floor_to_int(converted_data[field])

            # 階数文字列を整数に変換
            for field in ['construction_date']:
                if field in converted_data:
                    converted_data[field] = convert_japanese_era_date(converted_data[field])

            # 階数文字列を整数に変換
            for field in ['building_age']:
                if field in converted_data:
                    converted_data[field] = convert_building_age(converted_data[field])

            converted_properties.append(converted_data)

        table_id = f"{os.environ['PROJECT_ID']}.property_data.properties"
        errors = bq_client.insert_rows_json(table_id, converted_properties)
        if errors:
            logger.error(f"BigQueryデータ挿入エラー: {errors} 挿入データ: {json.dumps(converted_properties, indent=2, ensure_ascii=False)}")
            return False

        logger.info(f"{len(converted_properties)}件の物件データをBigQueryに保存しました")
        return True
    except Exception as e:
        logger.error(f"BigQueryへの保存中にエラーが発生: {str(e)}", exc_info=True)
        return False

def mark_as_read(service, message_id):
    """メールを既読にマーク"""
    try:
        service.users().messages().modify(
            userId='me',
            id=message_id,
            body={'removeLabelIds': ['UNREAD']}
        ).execute()
        logger.info(f"メールID {message_id} を既読にマークしました")
        return True
    except Exception as e:
        logger.error(f"メールID {message_id} の既読マーク処理でエラーが発生: {str(e)}", exc_info=True)
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

        # ヘッダー情報の抽出
        subject, from_email, date = extract_email_headers(msg['payload']['headers'])
        logger.debug(f"メールヘッダー: Subject='{subject}', From='{from_email}'")

        # 本文のデコード
        body = decode_email_body(msg['payload'])
        if not body:
            logger.info(f"メールID {message_data['id']}: 本文が空のためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        # 不動産関連チェック
        if not ('不動産' in body or '物件' in body):
            logger.info(f"メールID {message_data['id']}: 不動産関連キーワードなしのためスキップ")
            mark_as_read(service, message_data['id'])
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
        logger.debug("Gemini分析用データ:")
        logger.debug(f"subject: {subject}")
        logger.debug(f"body: {body}")

        # TODO: Geminiの出力上限に達すると、途中までで結果が返ってきてJSONが不完全でJSONパースエラーが発生する
        # 出力上限に達する前に返してもらってフラグで続きをリクエストするか制御が必要。ただし現在の1.5-flashだと意図通り実現できず
        # 次の安価のモデルが出たら試す、もしくは出力上限が拡大されたモデルならそもそも対応が不要になる
        # 現在はパースエラーになった時は既読にしてスキップさせる
        property_data_list = analyze_email_with_gemini(model, body, subject)

        if not property_data_list:
            logger.info(f"メールID {message_data['id']}: Gemini分析結果が空のためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        logger.debug(f"Gemini分析結果: {json.dumps(property_data_list, indent=2)}")

        filtered_properties = filter_valid_properties(property_data_list)
        if not filtered_properties:
            logger.info(f"メールID {message_data['id']}: 有効な物件データなしのためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        # データの準備
        processed_properties = []
        for property_data in filtered_properties:
            extended_data = prepare_property_data(property_data, email_info)
            if extended_data:
                processed_properties.append(extended_data)
            else:
                logger.warning("物件データの準備に失敗")

        if not processed_properties:
            logger.info(f"メールID {message_data['id']}: 処理可能な物件データなしのためスキップ")
            mark_as_read(service, message_data['id'])
            return None

        # BigQueryへの保存
        logger.info("BigQueryへの保存を開始")
        if not save_to_bigquery(bq_client, processed_properties):
            logger.error("BigQueryへの保存に失敗")
            return None

        # メールを既読にマーク
        if not mark_as_read(service, message_data['id']):
            logger.warning("既読マークに失敗")

        logger.info(f"メールID {message_data['id']} の処理が完了")
        return email_info

    except Exception as e:
        logger.error(f"""
        メールID {message_data['id']} の処理中にエラーが発生
        エラータイプ: {type(e).__name__}
        エラーメッセージ: {str(e)}
        """, exc_info=True)
        return None

def process_unread_property_emails(service, model, bq_client):
    """未読の不動産関連メールを処理"""
    logger.info("未読不動産メールの処理を開始")
    processed_count = 0

    try:
        # 未読メールの検索
        results = service.users().messages().list(
            userId='me',
            q='is:unread label:不動産',
            maxResults=100
        ).execute()

        if not results.get('messages'):
            logger.info("未読メールはありません")
            return [], 0

        messages = results.get('messages', [])
        logger.info(f"未読メール数: {len(messages)}")

        processed_emails = []
        for index, message in enumerate(messages, 1):
            logger.info(f"メール {index}/{len(messages)} の処理を開始")

            result = process_property_email(message, service, model, bq_client)
            if result:
                processed_emails.append(result)
                processed_count += 1
                logger.info(f"メール {index}/{len(messages)} の処理が完了")
            else:
                logger.warning(f"メール {index}/{len(messages)} の処理がスキップまたは失敗")

        logger.info(f"未読メール処理完了 (成功: {processed_count}/{len(messages)})")
        return processed_emails, processed_count

    except Exception as e:
        logger.error(f"未読メール処理中にエラーが発生: {str(e)}", exc_info=True)
        return [], 0

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
        error_message = f"メール処理中に重大なエラーが発生: {str(e)}"
        logger.critical(error_message, exc_info=True)
        return {
            'status': 'error',
            'message': error_message,
            'processed_emails': 0,
            'total_emails': 0
        }, 500