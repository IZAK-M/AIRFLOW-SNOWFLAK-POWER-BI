from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore


# map function if you want to sync with daily static DAG
"""def map_to_2am(execution_date, **_):
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)"""


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="GTFS_GOLD_2",
    start_date=datetime(2025, 9, 11),
    schedule="*/5 * * * *",  # Éxécuter toutes les 5 minutes pour actualiser les agrégats
    catchup=False,
    default_args=default_args,
    tags=["GTFS", "gold", "Snowflake"],
) as dag:

    # ---- s'assure que le schéma existe ----
    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="snowflake_conn",
        sql="CREATE SCHEMA IF NOT EXISTS GTFS_DB.GOLD;",
    )
    # -----------------------------
    # TABLES ET TRANSFORMATION SQL
    # -----------------------------

    # 1) Retards moyens dans le temps pour tous les voyages effectués dans la journée
    create_retard_moyen_par_min_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.retard_moyen_par_min (
        event_ts TIMESTAMP_NTZ,
        avg_delay_min FLOAT,
        observations NUMBER,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """
    insert_retard_moyen_par_min_sql = """
    INSERT INTO GTFS_DB.GOLD.retard_moyen_par_min (event_ts, avg_delay_min, observations)
    WITH
    scheduled AS (
        SELECT
            trip_id,
            stop_id,
            stop_sequence,
            TRY_TO_TIMESTAMP_NTZ(TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD') || ' ' || intermediate_stop, 'YYYY-MM-DD HH24:MI:SS') AS scheduled_ts
        FROM GTFS_DB.SILVER.stop_times_silver
    ),
    actual AS (
        SELECT
            trip_id,
            stop_id,
            stop_sequence,
            TRY_TO_TIMESTAMP_NTZ(intermediate_stop, 'YYYY-MM-DD HH24:MI:SS.FF3') AS actual_ts
        FROM GTFS_DB.SILVER.trip_stops_times_silver
    ),
    joined AS (
        SELECT
            a.trip_id,
            a.stop_id,
            a.stop_sequence,
            s.scheduled_ts,
            a.actual_ts,
            DATEDIFF('second', s.scheduled_ts, a.actual_ts) / 60.0 AS delay_min,
            DATE_TRUNC('day', a.actual_ts) AS event_day,
            a.actual_ts AS event_ts
        FROM actual a
        JOIN scheduled s
        ON a.trip_id = s.trip_id
        AND a.stop_sequence = s.stop_sequence
        WHERE s.scheduled_ts IS NOT NULL
        AND a.actual_ts IS NOT NULL
        -- optionnel : filtrer valeurs aberrantes
        -- AND DATEDIFF('minute', s.scheduled_ts, a.actual_ts) BETWEEN -120 AND 720
    ),
    binned AS (
        SELECT
            DATE_TRUNC('minute', event_ts) AS period_ts,
            delay_min
        FROM joined
        WHERE event_day = TO_DATE(TO_CHAR(CURRENT_DATE(), 'YYYY-MM-DD'))
    )

    SELECT
        period_ts AS event_ts,
        ROUND(AVG(delay_min), 2) AS avg_delay_min,
        COUNT(*) AS observations
    FROM binned
    GROUP BY period_ts
    ORDER BY period_ts;

    """
    create_carte_bus_rt_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.carte_bus_rt(
        vehicle_id STRING,
        trip_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        source_insert_date TIMESTAMP_NTZ,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('UTC','Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
        );
    """

    insert_carte_bus_rt_sql = """
    DELETE FROM GTFS_DB.GOLD.carte_bus_rt;
    INSERT INTO GTFS_DB.GOLD.carte_bus_rt (vehicle_id, trip_id, latitude, longitude, source_insert_date)
    WITH ranked AS (
    SELECT
        vehicle_id,
        trip_id,
        latitude,
        longitude,
        insert_date,
        ROW_NUMBER() OVER (PARTITION BY vehicle_id ORDER BY insert_date DESC) AS rn
    FROM GTFS_DB.SILVER.vehicle_positions_silver
    )
    SELECT 
        vehicle_id,
        trip_id,
        latitude,
        longitude,
        insert_date,
    FROM ranked
    WHERE rn = 1;
    """
    # -----------------------------------
    # Déclarations des Tasks
    # -----------------------------------
    create_retard_moyen_par_min = SQLExecuteQueryOperator(
        task_id="create_retard_moyen_par_min",
        conn_id="snowflake_conn",
        sql=create_retard_moyen_par_min_sql,
    )
    insert_retard_moyen_par_min = SQLExecuteQueryOperator(
        task_id="insert_retard_moyen_par_min",
        conn_id="snowflake_conn",
        sql=insert_retard_moyen_par_min_sql,
    )

    create_carte_bus_rt = SQLExecuteQueryOperator(
        task_id="create_carte_bus_rt",
        conn_id="snowflake_conn",
        sql=create_carte_bus_rt_sql,
    )
    insert_carte_bus_rt = SQLExecuteQueryOperator(
        task_id="insert_carte_bus_rt",
        conn_id="snowflake_conn",
        sql=insert_carte_bus_rt_sql,
    )

    # -----------------------------------
    # Orchestration: create schema -> create tables -> inserts.
    # -----------------------------------

    (
        create_gold_schema
        >> create_retard_moyen_par_min
        >> create_carte_bus_rt
        >> insert_retard_moyen_par_min
        >> insert_carte_bus_rt
    )
