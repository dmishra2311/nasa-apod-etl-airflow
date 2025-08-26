from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import requests
import pandas as pd
import os
import psycopg2
from psycopg2.extras import execute_values

DATA_DIR = "C:/Users/debad/nyc-subway-etl/"

# PostgreSQL connection config
PG_HOST = "postgres"
PG_DB = "airflow"
PG_USER = "airflow"
PG_PASSWORD = "airflow"
PG_PORT = 5432

def fetch_and_store_apod():
    # 1. Fetch APOD
    url = "https://api.nasa.gov/planetary/apod"
    params = {"api_key": "LekFCyDc5oEo2QgRO6hmSlScd7I6blZw9KoUvppI", "count": 1}  # Replace with your key
    resp = requests.get(url, params=params)
    data = resp.json()[0]

    os.makedirs(DATA_DIR, exist_ok=True)

    # 2. Download image (if media_type is image)
    image_path = None
    if data["media_type"] == "image":
        img_data = requests.get(data["url"]).content
        image_path = os.path.join(DATA_DIR, f"{data['date']}.jpg")
        with open(image_path, "wb") as f:
            f.write(img_data)
    
    # 3. Load metadata into Postgres
    df = pd.DataFrame([{
        "date": data["date"],
        "title": data["title"],
        "explanation": data["explanation"],
        "url": data["url"],
        "media_type": data["media_type"],
        "image_path": image_path
    }])

    # Connect to Postgres
    conn = psycopg2.connect(
        host=PG_HOST,
        database=PG_DB,
        user=PG_USER,
        password=PG_PASSWORD,
        port=PG_PORT
    )
    cur = conn.cursor()

    # Create table if not exists
    cur.execute("""
        CREATE TABLE IF NOT EXISTS nasa_apod (
            date DATE PRIMARY KEY,
            title TEXT,
            explanation TEXT,
            url TEXT,
            media_type TEXT,
            image_path TEXT
        )
    """)
    conn.commit()

    # Insert data
    records = df.to_dict('records')
    for record in records:
        cur.execute("""
            INSERT INTO nasa_apod (date, title, explanation, url, media_type, image_path)
            VALUES (%(date)s, %(title)s, %(explanation)s, %(url)s, %(media_type)s, %(image_path)s)
            ON CONFLICT (date) DO NOTHING
        """, record)

    conn.commit()
    cur.close()
    conn.close()

    print(f"Stored APOD {data['date']} in Postgres and image locally.")

# 4. Airflow DAG
with DAG(
    "nasa_apod_etl",
    start_date=datetime(2025, 8, 25),
    schedule_interval="@daily",
    catchup=False
) as dag:

    etl_task = PythonOperator(
        task_id="fetch_and_store_apod",
        python_callable=fetch_and_store_apod
    )
