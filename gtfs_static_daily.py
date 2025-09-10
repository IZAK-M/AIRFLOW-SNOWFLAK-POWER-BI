from airflow import DAG
from airflow.utils.task_group import TaskGroup
from airflow.operators.python import PythonOperator # type: ignore
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
import os
import requests
from dotenv import load_dotenv
import zipfile
import logging

# Charger le .env
load_dotenv()

# URL
GTFS_STATIC_URL = os.getenv("GTFS_STATIC_URL")

# Dossiers dans le conteneur
DATA_DIR = "/opt/airflow/data"
EXPORTS_DIR = "/opt/airflow/exports"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORTS_DIR, exist_ok=True)

UA = {"User-Agent": "airflow-gtfs-demo/1.0"}

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_gtfs_static_zip():
    """Télécharge l'archive GTFS statique dans DATA_DIR/gtfs_static.zip."""
    os.makedirs(DATA_DIR, exist_ok=True)
    resp = requests.get(GTFS_STATIC_URL, timeout=30, headers=UA)
    resp.raise_for_status()
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    with open(zip_path, "wb") as f:
        f.write(resp.content)
    print(f"Archive téléchargée : {zip_path}")
    return zip_path


def unzip_gtfs_static_zip():
    """Dézippe le GTFS statique dans EXPORTS_DIR/ (stops.txt, routes.txt, trips.txt, ...)."""
    zip_path = os.path.join(DATA_DIR, "gtfs_static.zip")
    if not os.path.exists(zip_path):
        raise FileNotFoundError(f"Archive manquante : {zip_path}")
    os.makedirs(EXPORTS_DIR, exist_ok=True)
    with zipfile.ZipFile(zip_path, "r") as zip_ref:
        zip_ref.extractall(EXPORTS_DIR)
    print(f"Contenu du GTFS static extrait dans {EXPORTS_DIR}")
    return EXPORTS_DIR


# SQL DDL pour Snowflake
create_db_and_schema_sql = [
    "CREATE DATABASE IF NOT EXISTS GTFS_DB;",
    "CREATE SCHEMA IF NOT EXISTS GTFS_DB.BRONZE;",
]

# Création des tables BRONZE pour le statique
create_bronze_static_tables_sql = [
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.routes (
        route_id STRING,
        agency_id STRING,
        route_short_name STRING,
        route_long_name STRING,
        route_type NUMBER,
        route_url STRING,
        route_color STRING,
        route_text_color STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)

    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trips (
        route_id STRING,
        service_id STRING,
        trip_id STRING,
        trip_headsign STRING,
        trip_short_name STRING,
        direction_id STRING,
        shape_id STRING,
        wheelchair_accessible STRING,
        bikes_allowed NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.stops (
        stop_id STRING,
        stop_code STRING,
        stop_name STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        zone_id STRING,
        location_type STRING,
        parent_station STRING,
        stop_timezone STRING,
        wheelchair_boarding STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.stop_times (
        trip_id STRING,
        arrival_time STRING,
        departure_time STRING,
        stop_id STRING,
        stop_sequence NUMBER,
        pickup_type STRING,
        drop_off_type STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)  
    );
    """
]

with DAG(
    dag_id="gtfs_static_daily",
    start_date=datetime(2025, 9, 3),
    schedule="@daily",  # Tous les jours à 3h
    catchup=False,
    tags=["GTFS", "static", "Snowflake"],
) as dag:

    task_download_gtfs_static = PythonOperator(
        task_id="download_gtfs_static_zip",
        python_callable=download_gtfs_static_zip,
    )
    task_unzip_gtfs_static = PythonOperator(
        task_id="unzip_gtfs_static_zip",
        python_callable=unzip_gtfs_static_zip,
    )

    # Petit ping SQL pour valider la connexion Snowflake
    snowflake_ping = SQLExecuteQueryOperator(
        task_id="snowflake_ping",
        conn_id="snowflake_conn",
        sql="""
            SELECT
              CURRENT_ACCOUNT(),
              CURRENT_REGION(),
              CURRENT_ROLE(),
              CURRENT_WAREHOUSE(),
              CURRENT_DATABASE(),
              CURRENT_SCHEMA();
        """,
        do_xcom_push=False,
    )

    create_db_and_schema = SQLExecuteQueryOperator(
        task_id="create_db_and_schema",
        conn_id="snowflake_conn",
        sql=create_db_and_schema_sql,
        do_xcom_push=False,
    )

    create_bronze_static_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_static_tables",
        conn_id="snowflake_conn",
        sql=create_bronze_static_tables_sql,
        do_xcom_push=False,
    )

     # Création d’un stage interne
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id="snowflake_conn",
        sql="CREATE OR REPLACE STAGE GTFS_DB.BRONZE.stage_gtfs_static;",
        do_xcom_push=False,
    )

    # Upload des fichiers extraits dans le stage
    put_routes = SQLExecuteQueryOperator(
        task_id="put_routes",
        conn_id="snowflake_conn",
        sql=f"PUT file://{EXPORTS_DIR}/routes.txt @GTFS_DB.BRONZE.stage_gtfs_static OVERWRITE = TRUE;",
        do_xcom_push=False,
    )

    put_trips = SQLExecuteQueryOperator(
        task_id="put_trips",
        conn_id="snowflake_conn",
        sql=f"PUT file://{EXPORTS_DIR}/trips.txt @GTFS_DB.BRONZE.stage_gtfs_static OVERWRITE = TRUE;",
        do_xcom_push=False,
    )

    put_stops = SQLExecuteQueryOperator(
        task_id="put_stops",
        conn_id="snowflake_conn",
        sql=f"PUT file://{EXPORTS_DIR}/stops.txt @GTFS_DB.BRONZE.stage_gtfs_static OVERWRITE = TRUE;",
        do_xcom_push=False,
    )

    put_stop_times = SQLExecuteQueryOperator(
        task_id="put_stop_times",
        conn_id="snowflake_conn",
        sql=f"PUT file://{EXPORTS_DIR}/stop_times.txt @GTFS_DB.BRONZE.stage_gtfs_static OVERWRITE = TRUE;",
        do_xcom_push=False,
    )

    # COPY INTO pour charger les fichiers dans Snowflake
    copy_routes = SQLExecuteQueryOperator(
        task_id="copy_routes",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.routes(route_id,agency_id,route_short_name,route_long_name,route_type,route_url,route_color,route_text_color)
            FROM @GTFS_DB.BRONZE.stage_gtfs_static/routes.txt
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    copy_trips = SQLExecuteQueryOperator(
        task_id="copy_trips",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.trips(route_id,service_id,trip_id,trip_headsign,trip_short_name,direction_id,shape_id,wheelchair_accessible,bikes_allowed)
            FROM @GTFS_DB.BRONZE.stage_gtfs_static/trips.txt
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    copy_stops = SQLExecuteQueryOperator(
        task_id="copy_stops",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.stops(stop_id,stop_code,stop_name,stop_lat,stop_lon,zone_id,location_type,parent_station,stop_timezone,wheelchair_boarding)
            FROM @GTFS_DB.BRONZE.stage_gtfs_static/stops.txt
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    copy_stop_times = SQLExecuteQueryOperator(
        task_id="copy_stop_times",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.stop_times(trip_id,arrival_time,departure_time,stop_id,stop_sequence,pickup_type,drop_off_type)
            FROM @GTFS_DB.BRONZE.stage_gtfs_static/stop_times.txt
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    # Ordre d’exécution
    task_download_gtfs_static >> task_unzip_gtfs_static >> snowflake_ping >> create_db_and_schema >> create_bronze_static_tables
    create_bronze_static_tables >> create_stage
    create_stage >> [put_routes, put_trips, put_stops, put_stop_times]
    put_routes >> copy_routes
    put_trips >> copy_trips
    put_stops >> copy_stops
    put_stop_times >> copy_stop_times