import base64
import functions_framework
import google.cloud.bigquery as bigquery
import google.generativeai as genai
import json
import os
import uuid
from datetime import datetime


def setup_gemini():
    """Gemini APIのセットアップ"""
    genai.configure(api_key=os.environ.get('GEMINI_API_KEY'))
    return genai.GenerativeModel('gemini-pro')


def analyze_email_with_gemini(model, email_content, email_subject):
    """Geminiを使用してメールの内容を解析"""
    prompt = f"""
    以下の不動産物件情報メールから、必要な情報を抽出してJSON形式で返してください。
    未記載の項目はnullとしてください。

    メールタイトル：
    {email_subject}

    メール本文：
    {email_content}

    以下の形式で出力してください：
    {{
        "property_name": "物件名",
        "property_type": "物件種別",
        "postal_code": "郵便番号",
        "prefecture": "都道府県",
        "city": "市区町村",
        "address": "番地以降の住所",
        "price": 価格（数値）,
        "monthly_fee": 月額費用（数値）,
        "management_fee": 管理費（数値）,
        "floor_area": 専有面積（数値）,
        "floor_number": 階数（数値）,
        "total_floors": 総階数（数値）,
        "nearest_station": "最寄駅",
        "station_distance": 駅までの距離（数値）,
        "building_age": 築年数（数値）,
        "construction_date": "建築年月日（YYYY-MM-DD形式）",
        "features": ["設備1", "設備2"],
        "status": "募集中",
        "source_company": "情報提供会社",
        "company_phone": "電話番号",
        "company_email": "メールアドレス",
        "property_url": "URL",
        "road_price": 路線価（数値）,
        "estimated_price": 積算価格（数値）,
        "current_rent_income": 現況家賃収入（数値）,
        "expected_rent_income": 想定家賃収入（数値）,
        "yield_rate": 利回り（数値）,
        "land_area": 敷地面積（数値）
    }}
    """

    response = model.generate_content(prompt)
    return json.loads(response.text)


@functions_framework.cloud_event
def process_property_emails(cloud_event):
    """Cloud Functionsのメインハンドラー"""
    try:
        # メールデータの取得（Pub/Subからのメッセージ）
        pubsub_message = base64.b64decode(cloud_event.data["message"]["data"]).decode()
        email_data = json.loads(pubsub_message)

        # Geminiのセットアップ
        model = setup_gemini()

        # メールの解析
        analysis_result = analyze_email_with_gemini(
            model,
            email_data.get('content', ''),
            email_data.get('subject', '')
        )

        # BigQueryクライアントの初期化
        client = bigquery.Client()

        # 必須フィールドの追加
        analysis_result.update({
            'id': str(uuid.uuid4()),
            'email_id': email_data.get('id', ''),
            'email_subject': email_data.get('subject', ''),
            'email_body': email_data.get('content', ''),
            'email_received_at': datetime.now().isoformat(),
            'email_from': email_data.get('from', ''),
            'created_at': datetime.now().isoformat(),
            'updated_at': datetime.now().isoformat()
        })

        # BigQueryに保存
        table_id = f"{os.environ['PROJECT_ID']}.property_data.properties"
        errors = client.insert_rows_json(table_id, [analysis_result])

        if errors:
            raise Exception(f"BigQuery insertion errors: {errors}")

    except Exception as e:
        # エラーログの保存
        error_data = {
            'error_id': str(uuid.uuid4()),
            'email_id': email_data.get('id', ''),
            'error_type': type(e).__name__,
            'error_message': str(e),
            'email_content': email_data.get('content', ''),
            'created_at': datetime.now().isoformat()
        }

        error_table_id = f"{os.environ['PROJECT_ID']}.property_data.error_logs"
        client.insert_rows_json(error_table_id, [error_data])

        raise e
