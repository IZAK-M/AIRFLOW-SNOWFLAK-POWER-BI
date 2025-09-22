from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime
from airflow.sensors.external_task import ExternalTaskSensor # type: ignore

# Fonctions de mapping pour synchronisation avec les autres DAGs
def map_to_2am(execution_date, **_):
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)

#POUR TABLES STATIC 

create_routes_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.routes_silver (
        route_id STRING,
        agency_id STRING,
        route_long_name STRING,
        route_type NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_routes_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.routes_silver (route_id, agency_id, route_long_name, route_type)
    SELECT route_id, agency_id, route_long_name, route_type
    FROM GTFS_DB.BRONZE.routes;
"""

create_trips_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.trips_silver (
        route_id STRING,
        service_id STRING,
        trip_id STRING,
        trip_headsign STRING,
        direction_id STRING,
        shape_id STRING,
        wheelchair_accessible NUMBER,
        bikes_allowed NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_trips_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.trips_silver (route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible, bikes_allowed)
    SELECT route_id, service_id, trip_id, trip_headsign, direction_id, shape_id, wheelchair_accessible, bikes_allowed
    FROM GTFS_DB.BRONZE.trips;
"""

create_stops_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.stops_silver (
        stop_id STRING,
        stop_code STRING,
        stop_name STRING,
        stop_lat FLOAT,
        stop_lon FLOAT,
        parent_station STRING,
        wheelchair_boarding STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_stops_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.stops_silver (stop_id, stop_code, stop_name, stop_lat, stop_lon, parent_station, wheelchair_boarding)
    SELECT stop_id, stop_code, stop_name, stop_lat, stop_lon, parent_station, wheelchair_boarding
    FROM GTFS_DB.BRONZE.stops;
"""

create_stop_times_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.stop_times_silver (
        trip_id STRING,
        stop_id STRING,
        stop_sequence NUMBER,
        intermediate_stop STRING,
        pickup_type STRING,
        drop_off_type STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_stop_times_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.stop_times_silver (trip_id, stop_id, stop_sequence, intermediate_stop, pickup_type, drop_off_type)
    SELECT trip_id, stop_id, stop_sequence, COALESCE(arrival_time, departure_time) AS intermediate_stop, pickup_type, drop_off_type
    FROM GTFS_DB.BRONZE.stop_times;
"""

# ====================== SQL POUR TABLES REALTIME ======================

create_trip_updates_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.trip_updates_silver (
        trip_id STRING,
        route_id STRING,
        direction_id NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_trip_updates_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.trip_updates_silver (trip_id, route_id, direction_id)
    SELECT trip_id, route_id, direction_id
    FROM GTFS_DB.BRONZE.trip_updates;
"""

create_trip_stops_times_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.trip_stops_times_silver (
        trip_id STRING,
        stop_id STRING,
        stop_sequence NUMBER,
        intermediate_stop STRING,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
"""

insert_trip_stops_times_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.trip_stops_times_silver (trip_id, stop_id, stop_sequence, intermediate_stop)
    SELECT 
        trip_id, 
        stop_id, 
        stop_sequence, 
        CONVERT_TIMEZONE(
         'UTC',
         'Europe/Paris',
         TO_TIMESTAMP_NTZ(
            COALESCE(arrival_time, departure_time)
         )
        ) AS intermediate_stop
    FROM GTFS_DB.BRONZE.trip_stops_times;
"""

create_vehicle_positions_silver_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.SILVER.vehicle_positions_silver (
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

insert_vehicle_positions_silver_sql = """
    INSERT INTO GTFS_DB.SILVER.vehicle_positions_silver (trip_id, route_id, vehicle_id, latitude, longitude, bearing, stop_id)
    SELECT trip_id, route_id, vehicle_id, latitude, longitude, bearing, stop_id
    FROM GTFS_DB.BRONZE.vehicle_positions;
"""

#DAG

with DAG(
    dag_id="gtfs_silver_insert",
    start_date=datetime(2025, 9, 11),
    schedule="*/5 * * * *",  # toutes les 5 minutes
    catchup=False,
    tags=["GTFS", "silver", "Snowflake"],
) as dag:

    wait_for_static_today = ExternalTaskSensor(
        task_id="wait_for_static_today",
        external_dag_id="gtfs_static_daily",
        external_task_id="unzip_gtfs_static_zip",
        execution_date_fn=map_to_2am,
        poke_interval=60,
        timeout=3600,
        mode="reschedule",
    )

    create_silver_schema = SQLExecuteQueryOperator(
        task_id="create_silver_schema",
        conn_id="snowflake_conn",
        sql="CREATE SCHEMA IF NOT EXISTS GTFS_DB.SILVER;",
    )

    # On déclare les opérateurs en utilisant les variables SQL définies en haut
    create_routes_silver = SQLExecuteQueryOperator(
        task_id="create_routes_silver", 
        conn_id="snowflake_conn", 
        sql=create_routes_silver_sql)
    
    insert_routes_silver = SQLExecuteQueryOperator(
        task_id="insert_routes_silver", 
        conn_id="snowflake_conn", 
        sql=insert_routes_silver_sql)

    create_trips_silver = SQLExecuteQueryOperator(
        task_id="create_trips_silver", 
        conn_id="snowflake_conn", 
        sql=create_trips_silver_sql)
    
    insert_trips_silver = SQLExecuteQueryOperator(
        task_id="insert_trips_silver", 
        conn_id="snowflake_conn", 
        sql=insert_trips_silver_sql)

    create_stops_silver = SQLExecuteQueryOperator(
        task_id="create_stops_silver", 
        conn_id="snowflake_conn", 
        sql=create_stops_silver_sql)
    
    insert_stops_silver = SQLExecuteQueryOperator(
        task_id="insert_stops_silver", 
        conn_id="snowflake_conn", 
        sql=insert_stops_silver_sql)

    create_stop_times_silver = SQLExecuteQueryOperator(
        task_id="create_stop_times_silver", 
        conn_id="snowflake_conn", 
        sql=create_stop_times_silver_sql)
    
    insert_stop_times_silver = SQLExecuteQueryOperator(
        task_id="insert_stop_times_silver", 
        conn_id="snowflake_conn", 
        sql=insert_stop_times_silver_sql)

    create_trip_updates_silver = SQLExecuteQueryOperator(
        task_id="create_trip_updates_silver", 
        conn_id="snowflake_conn", 
        sql=create_trip_updates_silver_sql)
    
    insert_trip_updates_silver = SQLExecuteQueryOperator(
        task_id="insert_trip_updates_silver", 
        conn_id="snowflake_conn", 
        sql=insert_trip_updates_silver_sql)

    create_trip_stops_times_silver = SQLExecuteQueryOperator(
        task_id="create_trip_stops_times_silver",
        conn_id="snowflake_conn", 
        sql=create_trip_stops_times_silver_sql)
    
    insert_trip_stops_times_silver = SQLExecuteQueryOperator(
        task_id="insert_trip_stops_times_silver", 
        conn_id="snowflake_conn", 
        sql=insert_trip_stops_times_silver_sql)

    create_vehicle_positions_silver = SQLExecuteQueryOperator(
        task_id="create_vehicle_positions_silver", 
        conn_id="snowflake_conn", 
        sql=create_vehicle_positions_silver_sql)
    
    insert_vehicle_positions_silver = SQLExecuteQueryOperator(
        task_id="insert_vehicle_positions_silver", 
        conn_id="snowflake_conn", 
        sql=insert_vehicle_positions_silver_sql)

    # Orchestration
    wait_for_static_today >> create_silver_schema >> create_routes_silver >> create_trips_silver >> create_stops_silver >> create_stop_times_silver >> create_trip_updates_silver >> create_trip_stops_times_silver >> create_vehicle_positions_silver >> insert_routes_silver >> insert_trips_silver >> insert_stops_silver >> insert_stop_times_silver >> insert_trip_updates_silver >> insert_trip_stops_times_silver >> insert_vehicle_positions_silver