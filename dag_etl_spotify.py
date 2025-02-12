import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import base64
import sqlalchemy
import pandas as pd
import json


# Налаштування логування
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Константи
CLIENT_ID = ''
CLIENT_SECRET = ''
REDIRECT_URI = 'http://localhost:3000'
AUTHORIZATION_CODE = '' 
DATABASE_LOCATION = ''

# Функція отримання токена
def get_token(ti):
    logger.info("Отримання Spotify токена...")
    
    auth_str = f"{CLIENT_ID}:{CLIENT_SECRET}"
    auth_base64 = base64.b64encode(auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {auth_base64}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    data = {
        "grant_type": "authorization_code",
        "code": AUTHORIZATION_CODE,
        "redirect_uri": REDIRECT_URI
    }

    response = requests.post("https://accounts.spotify.com/api/token", headers=headers, data=data)

    if response.status_code == 200:
        token_info = response.json()
        access_token = token_info["access_token"]
        ti.xcom_push(key="access_token", value=access_token)
        logger.info(f"Отримано токен: {access_token[:10]}... (обрізано)")
    else:
        logger.error(f"Помилка отримання токена: {response.status_code} - {response.text}")
        raise Exception(f"Помилка отримання токена: {response.text}")

# Функція отримання даних із Spotify
def get_data_from_spotify(ti):
    logger.info("Отримання даних про прослухані треки...")
    
    token = ti.xcom_pull(task_ids='get_token', key="access_token")
    if not token:
        logger.error("Токен не отримано! Завершення.")
        raise Exception("Spotify API Token не отримано!")

    headers = {"Authorization": f"Bearer {token}"}

    yesterday_unix = int((datetime.now() - timedelta(days=1)).timestamp()) * 1000
    url = f"https://api.spotify.com/v1/me/player/recently-played?after={yesterday_unix}"
    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        logger.error(f"Помилка запиту до Spotify API: {response.status_code} - {response.text}")
        raise Exception(f"Spotify API error: {response.text}")

    data = response.json()
    logger.info(f"Отримано {len(data.get('items', []))} записів.")

    ti.xcom_push(key="spotify_data", value=data)

# Функція вставки даних у PostgreSQL
def insert_data_into_database(ti):
    logger.info("Вставка даних у базу PostgreSQL...")
    
    data = ti.xcom_pull(task_ids='get_data_from_spotify', key="spotify_data")
    if not data:
        logger.error("Дані від Spotify не отримано! Завершення.")
        raise Exception("Немає даних для запису в базу!")

    song_list = []
    for song in data.get("items", []):
        song_list.append({
            "song_name": song["track"]["name"],
            "artist_name": song["track"]["album"]["artists"][0]["name"],
            "played_at": song["played_at"],
            "timestamp": song["played_at"][:10]
        })

    df = pd.DataFrame(song_list)
    if df.empty:
        logger.warning("Немає нових прослуханих треків. Завершення.")
        return

    logger.info(f"Готуємо до запису {len(df)} записів у базу даних.")

    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    with engine.begin() as conn:
        conn.execute("""
        CREATE TABLE IF NOT EXISTS my_played_tracks (
            song_name VARCHAR(200),
            artist_name VARCHAR(200),
            played_at VARCHAR(200) PRIMARY KEY,
            timestamp VARCHAR(200)
        )
        """)

        for _, row in df.iterrows():
            conn.execute("""
            INSERT INTO my_played_tracks (song_name, artist_name, played_at, timestamp)
                VALUES (%s, %s, %s, %s)
                ON CONFLICT (played_at) DO UPDATE
                SET song_name = EXCLUDED.song_name, 
                    artist_name = EXCLUDED.artist_name,
                    timestamp = EXCLUDED.timestamp
            """, (row['song_name'], row['artist_name'], row['played_at'], row['timestamp']))

    logger.info("Дані успішно збережені у базі!")

# Налаштування DAG
default_args = {
    "owner": "airflow",
    "start_date": datetime(2024, 2, 7),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "spotify_etl",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False,
)

get_token_task = PythonOperator(
    task_id="get_token",
    python_callable=get_token,
    provide_context=True,
    dag=dag,
)

get_data_task = PythonOperator(
    task_id="get_data_from_spotify",
    python_callable=get_data_from_spotify,
    provide_context=True,
    dag=dag,
)

insert_data_task = PythonOperator(
    task_id="insert_data_into_database",
    python_callable=insert_data_into_database,
    provide_context=True,
    dag=dag,
)

get_token_task >> get_data_task >> insert_data_task