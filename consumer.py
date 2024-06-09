import json
from kafka import KafkaConsumer
from google.oauth2 import service_account
from googleapiclient.discovery import build
from datetime import datetime
import pytz

def initialize_google_sheets(spreadsheet_id, range_name, headers):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = '/home/jovyan/notebooks/projekt_rta/service_account.json'

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()

    body = {
        'values': [headers]
    }
    result = sheet.values().update(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"Zainicjowano arkusz Google Sheets z nagłówkami: {headers}")

def write_to_google_sheets(data, spreadsheet_id, range_name):
    SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
    SERVICE_ACCOUNT_FILE = '/home/jovyan/notebooks/projekt_rta/service_account.json'

    credentials = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=credentials)

    sheet = service.spreadsheets()

    values = [[data["order_time"], data["traffic_source"], data["campaign_name"], data["user_id"]]]
    body = {
        'values': values
    }
    result = sheet.values().append(
        spreadsheetId=spreadsheet_id,
        range=range_name,
        valueInputOption='RAW',
        body=body
    ).execute()
    print(f"Wpisane dane do Google Sheets: {data}")

if __name__ == "__main__":
    SERVER = "broker:9092"
    SPREADSHEET_ID = '1_fT_Aywuzbi8vALdAVOQqxeZKIYl6uynYbxadqS_brk'  
    RANGE_NAME = 'Arkusz3!A1:D1'  
    HEADERS = ["Order Time", "Traffic Source", "Campaign Name", "User ID"]

    initialize_google_sheets(SPREADSHEET_ID, RANGE_NAME, HEADERS)

    consumer = KafkaConsumer(
        'streaming',
        bootstrap_servers=[SERVER],
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='my_consumer_group',
        value_deserializer=lambda x: x.decode('utf-8')
    )

    print("Uruchamianie konsumenta Kafka...")
    try:
        for message in consumer:
            try:
                if message.value.strip():  
                    data = json.loads(message.value)
                    print(f"Skonsumowana wiadomość: {data}")

                    
                    expected_keys = ["order_time", "traffic_source", "campaign_name", "user_id"]
                    if all(key in data for key in expected_keys):
                        
                        order_time = datetime.strptime(data["order_time"], "%Y-%m-%d %H:%M:%S")
                        local_tz = pytz.timezone('Europe/Warsaw')
                        order_time = local_tz.localize(order_time)
                        data["order_time"] = order_time.strftime("%Y-%m-%d %H:%M:%S")

                        write_to_google_sheets(data, SPREADSHEET_ID, RANGE_NAME)
                    else:
                        print(f"Nieoczekiwany format wiadomości: {data}")
                else:
                    print("Otrzymano pustą wiadomość")
            except json.JSONDecodeError as e:
                print(f"Błąd dekodowania JSON: {e} - wiadomość: {message.value}")
    except KeyboardInterrupt:
        print("Zamykam konsumenta Kafka")
        consumer.close()









