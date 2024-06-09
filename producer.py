import json
import pandas as pd
from time import sleep
from kafka import KafkaProducer
from datetime import datetime
import random
import pytz

def read_csv(file_path):
    df = pd.read_csv(file_path)
    return df

if __name__ == "__main__":
    SERVER = "broker:9092"
    CSV_FILE_PATH = '/home/jovyan/notebooks/projekt_rta/data_orders_RTA.csv'
    CAMPAIGNS = ["summer_sale_15", "summer_sale_10", "summer_sale_5"]
    TIMEZONE = 'Europe/Warsaw'  

    producer = KafkaProducer(
        bootstrap_servers=[SERVER],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        api_version=(3, 7, 0),
    )

    df = read_csv(CSV_FILE_PATH)
    
    try:
        for index, row in df.iterrows():
            
            local_time = datetime.now(pytz.timezone(TIMEZONE))
            time_parsed = local_time.strftime("%Y-%m-%d %H:%M:%S")  
            
            campaign_name = random.choice(CAMPAIGNS)  

            message = {
                "order_time": time_parsed,  
                "traffic_source": row['traffic_source'],
                "campaign_name": campaign_name,
                "user_id": row['user_id']
            }
            
            producer.send("streaming", value=message)
            print(f"Wysłano wiadomość: {json.dumps(message)}") 
            sleep(1) 
    except KeyboardInterrupt:
        producer.close()






