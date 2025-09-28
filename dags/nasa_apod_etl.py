from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import os
import requests

NASA_API_KEY = os.getenv("NASA_API_KEY")
DATA_DIR = "/opt/airflow/data/images"

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

def fetch_and_store_apod(**context):
    if not NASA_API_KEY:
        raise ValueError("NASA_API_KEY not set in environment")

    url = "https://api.nasa.gov/planetary/apod"
    params = {"api_key": NASA_API_KEY, "count": 1}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()

    item = resp.json()[0]  # API returns a list when count=1
    record = {
        "apod_date": item.get("date"),
        "title": item.get("title"),
        "explanation": item.get("explanation"),
        "url": item.get("url"),
        "media_type": item.get("media_type")
    }

    # Download image if media_type == image
    image_path = None
    if record["media_type"] == "image" and record["url"]:
        os.makedirs(DATA_DIR, exist_ok=True)
        # choose extension from URL (fallback to .jpg)
        ext = os.path.splitext(record["url"].split('?')[0])[1] or ".jpg"
        filename = f"{record['apod_date']}{ext}"
        image_path = os.path.join(DATA_DIR, filename)
        # stream download
        r = requests.get(record["url"], timeout=30)
        r.raise_for_status()
        with open(image_path, "wb") as f:
            f.write(r.content)

    # Insert metadata into Postgres using PostgresHook
    pg = PostgresHook(postgres_conn_id="postgres_default")
    create_sql = """
    CREATE TABLE IF NOT EXISTS nasa_apod (
        apod_date DATE PRIMARY KEY,
        title TEXT,
        explanation TEXT,
        url TEXT,
        media_type TEXT,
        image_path TEXT
    );
    """
    pg.run(create_sql)

    insert_sql = """
    INSERT INTO nasa_apod (apod_date, title, explanation, url, media_type, image_path)
    VALUES (%s, %s, %s, %s, %s, %s)
    ON CONFLICT (apod_date) DO UPDATE
      SET title = EXCLUDED.title,
          explanation = EXCLUDED.explanation,
          url = EXCLUDED.url,
          media_type = EXCLUDED.media_type,
          image_path = EXCLUDED.image_path;
    """
    pg.run(insert_sql, parameters=(
        record["apod_date"],
        record["title"],
        record["explanation"],
        record["url"],
        record["media_type"],
        image_path
    ))

    # push to XCom if you want to inspect
    context["ti"].xcom_push(key="apod_record", value=record)

with DAG(
    "nasa_apod_etl",
    default_args=default_args,
    description="Fetch NASA APOD, download image, store metadata in Postgres",
    schedule_interval="@daily",
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=["nasa", "apod"],
) as dag:

    run_etl = PythonOperator(
        task_id="fetch_and_store_apod",
        python_callable=fetch_and_store_apod,
        provide_context=True
    )
 
