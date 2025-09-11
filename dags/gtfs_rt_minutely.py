from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
import os
import requests
from dotenv import load_dotenv
from google.transit import gtfs_realtime_pb2
import logging
import csv
from datetime import datetime
from zoneinfo import ZoneInfo
from airflow.operators.empty import EmptyOperator
from airflow.sensors.external_task import ExternalTaskSensor

load_dotenv()

GTFS_RT_TU_URL = os.getenv("GTFS_RT_TU_URL")
GTFS_RT_VP_URL = os.getenv("GTFS_RT_VP_URL")

#WAREHOUSE = "ETL_WH" 
#DATABASE = "GTFS_DB"
#SCHEMA = "BRONZE"

# Dossiers dans le conteneur
DATA_DIR = "/opt/airflow/data"
EXPORTS_DIR = "/opt/airflow/exports"
os.makedirs(DATA_DIR, exist_ok=True)
os.makedirs(EXPORTS_DIR, exist_ok=True)

UA = {"User-Agent": "airflow-gtfs-demo/1.0"}

# Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def download_file(url: str, filename: str):
    os.makedirs(DATA_DIR, exist_ok=True)
    resp = requests.get(url, timeout=30, headers=UA)
    resp.raise_for_status()
    path = os.path.join(DATA_DIR, filename)
    with open(path, "wb") as f:
        f.write(resp.content)
    print(f" Fichier téléchargé : {path}")
    return path

def export_trip_updates():
    pb_path = download_file(GTFS_RT_TU_URL, "trip_updates.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())
    out_txt = os.path.join(EXPORTS_DIR, "trip_updates.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                f.write(str(ent.trip_update) + "\n")
    print(f" Export TripUpdates : {out_txt}")
    return out_txt

def export_vehicle_positions():
    pb_path = download_file(GTFS_RT_VP_URL, "vehicle_positions.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())
    out_txt = os.path.join(EXPORTS_DIR, "vehicle_positions.txt")
    with open(out_txt, "w", encoding="utf-8") as f:
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                f.write(str(ent.vehicle) + "\n")
    print(f" Export VehiclePositions : {out_txt}")
    return out_txt

# TRANSFORMATIONS TEMPS RÉEL -> CSV
def transform_trip_updates_to_csv():
    pb_path = os.path.join(DATA_DIR, "trip_updates.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())

    #pour générer un suffixe avec la date et heure (format YYYYMMDD_HHMMSS)
    timestamp = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


    tu_csv = os.path.join(EXPORTS_DIR, f"trip_updates_{timestamp}.csv")
    with open(tu_csv, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["trip_id", "route_id", "direction_id"])
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                td = ent.trip_update.trip
                trip_id = td.trip_id if td.HasField("trip_id") else ""
                route_id = td.route_id if td.HasField("route_id") else ""
                direction_id = td.direction_id if td.HasField("direction_id") else None
                writer.writerow([trip_id, route_id, direction_id])

    tst_csv = os.path.join(EXPORTS_DIR, f"trip_stops_times_{timestamp}.csv")
    with open(tst_csv, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["trip_id", "stop_sequence", "stop_id", "arrival_time", "departure_time"])
        for ent in feed.entity:
            if ent.HasField("trip_update"):
                td = ent.trip_update.trip
                trip_id = td.trip_id if td.HasField("trip_id") else ""
                for stu in ent.trip_update.stop_time_update:
                    stop_seq = stu.stop_sequence if stu.HasField("stop_sequence") else ""
                    stop_id = stu.stop_id if stu.HasField("stop_id") else ""
                    arr_time = stu.arrival.time if stu.HasField("arrival") and stu.arrival.HasField("time") else ""
                    dep_time = stu.departure.time if stu.HasField("departure") and stu.departure.HasField("time") else ""
                    writer.writerow([trip_id, stop_seq, stop_id, arr_time, dep_time])

    print(f"CSV créés : {tu_csv} et {tst_csv}")
    return {"trip_updates_csv": tu_csv, "trip_stops_times_csv": tst_csv}

def transform_vehicle_positions_to_csv():
    pb_path = os.path.join(DATA_DIR, "vehicle_positions.pb")
    feed = gtfs_realtime_pb2.FeedMessage()
    with open(pb_path, "rb") as f:
        feed.ParseFromString(f.read())

    timestamp = datetime.now(ZoneInfo("Europe/Paris")).strftime("%Y%m%d_%H%M%S")


    vp_csv = os.path.join(EXPORTS_DIR, f"vehicle_positions_{timestamp}.csv")
    with open(vp_csv, "w", newline="", encoding="utf-8") as f_csv:
        writer = csv.writer(f_csv)
        writer.writerow(["trip_id", "route_id", "vehicle_id", "latitude", "longitude", "bearing", "stop_id"])
        for ent in feed.entity:
            if ent.HasField("vehicle"):
                v = ent.vehicle
                trip_id = v.trip.trip_id if v.HasField("trip") and v.trip.HasField("trip_id") else ""
                route_id = v.trip.route_id if v.HasField("trip") and v.trip.HasField("route_id") else ""
                vehicle_id = v.vehicle.id if v.HasField("vehicle") and v.vehicle.HasField("id") else ""
                lat = v.position.latitude if v.HasField("position") and v.position.HasField("latitude") else ""
                lon = v.position.longitude if v.HasField("position") and v.position.HasField("longitude") else ""
                bearing = v.position.bearing if v.HasField("position") and v.position.HasField("bearing") else ""
                stop_id = v.stop_id if v.HasField("stop_id") else ""
                writer.writerow([trip_id, route_id, vehicle_id, lat, lon, bearing, stop_id])

    print(f"CSV créé : {vp_csv}")
    return vp_csv

def map_to_midnight(execution_date, **context):
    # force à pointer vers l’exécution du DAG daily de la même journée
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)

# SQL pour tables BRONZE
create_bronze_rt_tables_sql = [
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trip_updates (
        trip_id STRING,
        route_id STRING,
        direction_id NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.trip_stops_times (
        trip_id STRING,
        stop_sequence NUMBER,
        stop_id STRING,
        arrival_time NUMBER,
        departure_time NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """,
    """
    CREATE TABLE IF NOT EXISTS GTFS_DB.BRONZE.vehicle_positions (
        trip_id STRING,
        route_id STRING,
        vehicle_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        stop_id STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """
]

with DAG(
    dag_id="gtfs_rt_minutely",
    start_date=datetime(2025, 9, 3),
    schedule="*/2 * * * *",  # toutes les 2 minutes */2 * * * *
    catchup=False,
    tags=["GTFS", "realtime", "Snowflake"],
) as dag:

    wait_for_static_today = ExternalTaskSensor(
        task_id="wait_for_static_today",
        external_dag_id="gtfs_static_daily",
        external_task_id="unzip_gtfs_static_zip", 
        execution_date_fn=map_to_midnight,
        allowed_states=["success"],
        failed_states=["failed", "skipped"],
        poke_interval=60,   # check toutes les 60s
        timeout=60*60,      # max 1h d’attente
        mode="reschedule",  # libère un worker pendant l’attente
    )

    task_trip_updates = PythonOperator(
        task_id="export_trip_updates",
        python_callable=export_trip_updates,
    )
    task_trip_stop_times= PythonOperator(
        task_id="export_trip_stop_times",
        python_callable=export_vehicle_positions,
    )
    task_vehicle_positions = PythonOperator(
        task_id="export_vehicle_positions",
        python_callable=export_vehicle_positions,
    )

    transform_tu_csv = PythonOperator(
        task_id="transform_trip_updates_to_csv",
        python_callable=transform_trip_updates_to_csv,
    )

    transform_st_csv = PythonOperator(
    task_id="transform_trip_stop_times_to_csv",
    python_callable=transform_trip_updates_to_csv,
    )

    transform_vp_csv = PythonOperator(
        task_id="transform_vehicle_positions_to_csv",
        python_callable=transform_vehicle_positions_to_csv,
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

    # Création de la base de données
    create_db = SQLExecuteQueryOperator(
        task_id="create_db",
        conn_id="snowflake_conn",
        sql="CREATE DATABASE IF NOT EXISTS GTFS_DB;",
        do_xcom_push=False,
    )

    # Création du schéma
    create_schema = SQLExecuteQueryOperator(
        task_id="create_schema",
        conn_id="snowflake_conn",
        sql="CREATE SCHEMA IF NOT EXISTS GTFS_DB.BRONZE;",
        do_xcom_push=False,
    )

    # Création des tables BRONZE
    create_bronze_rt_tables = SQLExecuteQueryOperator(
        task_id="create_bronze_rt_tables",
        conn_id="snowflake_conn",
        sql=create_bronze_rt_tables_sql,
        do_xcom_push=False,
    )

    # Création d'un stage interne pour uploader les fichiers
    create_stage = SQLExecuteQueryOperator(
        task_id="create_stage",
        conn_id="snowflake_conn",
        sql="CREATE OR REPLACE STAGE GTFS_DB.BRONZE.stage_gtfs_rt_minutely;",
        do_xcom_push=False,
    )

    # PUT : upload des fichiers locaux (CSV) vers le stage
    put_trip_updates = SQLExecuteQueryOperator(
        task_id="put_trip_updates",
        conn_id="snowflake_conn",
        sql=f"""
            PUT 'file://{EXPORTS_DIR}/trip_updates_*.csv'
            @GTFS_DB.BRONZE.stage_gtfs_rt_minutely AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
        """,
        do_xcom_push=False,
    )

    put_trip_stops_times = SQLExecuteQueryOperator(
        task_id="put_trip_stops_times",
        conn_id="snowflake_conn",
        sql=f"""
            PUT 'file://{EXPORTS_DIR}/trip_stops_times_*.csv'
            @GTFS_DB.BRONZE.stage_gtfs_rt_minutely AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
        """,
        do_xcom_push=False,
    )

    put_vehicle_positions = SQLExecuteQueryOperator(
        task_id="put_vehicle_positions",
        conn_id="snowflake_conn",
        sql=f"""
            PUT 'file://{EXPORTS_DIR}/vehicle_positions_*.csv'
            @GTFS_DB.BRONZE.stage_gtfs_rt_minutely AUTO_COMPRESS=FALSE OVERWRITE = TRUE;
        """,
        do_xcom_push=False,
    )


    # COPY INTO : charger les CSV dans les tables
    copy_trip_updates = SQLExecuteQueryOperator(
        task_id="copy_trip_updates",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.trip_updates(trip_id,route_id,direction_id)
            FROM @GTFS_DB.BRONZE.stage_gtfs_rt_minutely
            PATTERN = '.*trip_updates_.*\\.csv'
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    copy_trip_stops_times = SQLExecuteQueryOperator(
        task_id="copy_trip_stops_times",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.trip_stops_times(trip_id,stop_sequence,stop_id,arrival_time,departure_time)
            FROM @GTFS_DB.BRONZE.stage_gtfs_rt_minutely
            PATTERN = '.*trip_stops_times_.*\\.csv'
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
        """,
        do_xcom_push=False,
    )

    copy_vehicle_positions = SQLExecuteQueryOperator(
        task_id="copy_vehicle_positions",
        conn_id="snowflake_conn",
        sql="""
            COPY INTO GTFS_DB.BRONZE.vehicle_positions(trip_id,route_id,vehicle_id,latitude,longitude,bearing,stop_id)
            FROM @GTFS_DB.BRONZE.stage_gtfs_rt_minutely
            PATTERN = '.*vehicle_positions_.*\\.csv'
            FILE_FORMAT = (TYPE = CSV FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1);
           
        """,
        do_xcom_push=False,
    )

    # Ordre

    wait_for_static_today >> task_trip_updates >> task_trip_stop_times >> task_vehicle_positions >> transform_tu_csv >> transform_st_csv >> transform_vp_csv  >> snowflake_ping >> create_db >> create_schema >> create_bronze_rt_tables >> create_stage >> put_trip_updates >> put_trip_stops_times >> put_vehicle_positions >> copy_trip_updates >> copy_trip_stops_times >> copy_vehicle_positions





