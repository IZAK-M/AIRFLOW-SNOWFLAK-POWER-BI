from airflow import DAG
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from datetime import datetime, timedelta
from airflow.sensors.external_task import ExternalTaskSensor  # type: ignore


# map function if you want to sync with daily static DAG
def map_to_2am(execution_date, **_):
    return execution_date.replace(hour=0, minute=0, second=0, microsecond=0)


default_args = {
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="gtfs_gold_etl",
    start_date=datetime(2025, 9, 11),
    schedule="*/5 * * * *",  # run every 5 minutes to refresh RT aggregates
    catchup=False,
    default_args=default_args,
    tags=["GTFS", "gold", "Snowflake"],
) as dag:

    # ---- ensure schema exists ----
    create_gold_schema = SQLExecuteQueryOperator(
        task_id="create_gold_schema",
        conn_id="snowflake_conn",
        sql="CREATE SCHEMA IF NOT EXISTS GTFS_DB.GOLD;",
    )

    # -------------------------
    # TABLES AND TRANSFORM SQL
    # -------------------------

    # 1) vehicle_positions_latest: latest position per vehicle with computed delay estimate (delay_min from trip_updates join)
    create_vehicle_positions_latest_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.vehicle_positions_latest (
        vehicle_id STRING,
        trip_id STRING,
        route_id STRING,
        latitude FLOAT,
        longitude FLOAT,
        bearing FLOAT,
        stop_id STRING,
        event_ts TIMESTAMP_NTZ,
        delay_min FLOAT,
        insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_vehicle_positions_latest_sql = """
    INSERT INTO GTFS_DB.GOLD.vehicle_positions_latest (vehicle_id, trip_id, route_id, latitude, longitude, bearing, stop_id, event_ts, delay_min)
    SELECT
        vp.vehicle_id,
        vp.trip_id,
        vp.route_id,
        vp.latitude,
        vp.longitude,
        vp.bearing,
        vp.stop_id,
    CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(vp.insert_date)) AS event_ts,
    NULL::FLOAT AS delay_min
    FROM GTFS_DB.SILVER.vehicle_positions_silver vp;
    """

    # 2) trips_delay_timeseries: average delay per minute (or per five-minute window) for timeseries chart
    create_trips_delay_timeseries_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.trips_delay_timeseries (
      bucket_ts TIMESTAMP_NTZ,
      bucket_minute INT,
      avg_delay_min FLOAT,
      sample_count INT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_trips_delay_timeseries_sql = """
    INSERT INTO GTFS_DB.GOLD.trips_delay_timeseries (bucket_ts, bucket_minute, avg_delay_min, sample_count)
    SELECT
      DATE_TRUNC('minute', event_ts) AS bucket_ts,
      DATE_PART('minute', event_ts) AS bucket_minute,
      AVG(delay_min) AS avg_delay_min,
      COUNT(1) AS sample_count
    FROM (
      SELECT 
        CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(tp.insert_date)) AS event_ts,
        COALESCE(tp.delay_min, 0) AS delay_min
      FROM GTFS_DB.SILVER.trip_updates_silver tp
      UNION ALL
      SELECT
        event_ts,
        COALESCE(delay_min,0)
      FROM GTFS_DB.SILVER.vehicle_positions_silver vp
      LEFT JOIN GTFS_DB.SILVER.trip_updates_silver tu ON vp.trip_id = tu.trip_id
    )
    GROUP BY DATE_TRUNC('minute', event_ts);
    """

    # 3) stops_aggregate: avg delay per stop and last seen timestamp (for stops map & top N)
    create_stops_aggregate_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.stops_aggregate (
      stop_id STRING,
      stop_name STRING,
      nb_passages INT,
      avg_delay_min FLOAT,
      last_event_ts TIMESTAMP_NTZ,
      latitude FLOAT,
      longitude FLOAT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_stops_aggregate_sql = """
    INSERT INTO GTFS_DB.GOLD.stops_aggregate (stop_id, stop_name, nb_passages, avg_delay_min, last_event_ts, latitude, longitude)
    SELECT
      s.stop_id,
      s.stop_name,
      COUNT(distinct vp.trip_id || TO_CHAR(vp.insert_date)) AS nb_passages,
      AVG(COALESCE(tu.delay_min,0)) AS avg_delay_min,
      MAX(CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(vp.insert_date))) AS last_event_ts,
      s.stop_lat,
      s.stop_lon
    FROM GTFS_DB.SILVER.stops_silver s
    LEFT JOIN GTFS_DB.SILVER.vehicle_positions_silver vp ON vp.stop_id = s.stop_id
    LEFT JOIN GTFS_DB.SILVER.trip_updates_silver tu ON tu.trip_id = vp.trip_id
    GROUP BY s.stop_id, s.stop_name, s.stop_lat, s.stop_lon;
    """

    # 4) route_delay_agg: avg delay per route (for bar chart of most delayed lines)
    create_route_delay_agg_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.route_delay_agg (
      route_id STRING,
      avg_delay_min FLOAT,
      pct_late FLOAT,
      sample_count INT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_route_delay_agg_sql = """
    INSERT INTO GTFS_DB.GOLD.route_delay_agg (route_id, avg_delay_min, pct_late, sample_count)
    SELECT
      r.route_id,
      AVG(COALESCE(tu.delay_min,0)) AS avg_delay_min,
      CASE WHEN COUNT(1)=0 THEN 0 ELSE SUM(CASE WHEN COALESCE(tu.delay_min,0) > 5 THEN 1 ELSE 0 END) / COUNT(1)::FLOAT END AS pct_late,
      COUNT(1) AS sample_count
    FROM GTFS_DB.SILVER.routes_silver r
    LEFT JOIN GTFS_DB.SILVER.trip_updates_silver tu ON tu.route_id = r.route_id
    GROUP BY r.route_id;
    """

    # 5) heatmap_hour_weekday: average delay by hour x weekday
    create_heatmap_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.heatmap_hour_weekday (
      weekday INT, -- 1=Monday..7=Sunday (Snowflake DATE_PART('dow') can be adjusted)
      hour INT,
      avg_delay_min FLOAT,
      sample_count INT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_heatmap_sql = """
    INSERT INTO GTFS_DB.GOLD.heatmap_hour_weekday (weekday, hour, avg_delay_min, sample_count)
    SELECT
      DATE_PART('dow', event_ts) AS weekday,
      DATE_PART('hour', event_ts) AS hour,
      AVG(delay_min) AS avg_delay_min,
      COUNT(1) AS sample_count
    FROM (
      SELECT CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(insert_date)) AS event_ts, COALESCE(delay_min,0) AS delay_min
      FROM GTFS_DB.SILVER.trip_updates_silver
      UNION ALL
      SELECT CONVERT_TIMEZONE('UTC','Europe/Paris', TO_TIMESTAMP_NTZ(insert_date)) AS event_ts, COALESCE(NULL,0) FROM GTFS_DB.SILVER.vehicle_positions_silver
    )
    GROUP BY DATE_PART('dow', event_ts), DATE_PART('hour', event_ts);
    """

    # 6) punctuality_overview: percentage on-time (delay <=5) overall and by interval
    create_punctuality_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.punctuality_overview (
      interval_start TIMESTAMP_NTZ,
      interval_end TIMESTAMP_NTZ,
      pct_on_time FLOAT,
      total_count INT,
      on_time_count INT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_punctuality_sql = """
    -- example: compute punctuality over last 5 minutes
    INSERT INTO GTFS_DB.GOLD.punctuality_overview (interval_start, interval_end, pct_on_time, total_count, on_time_count)
    SELECT
      DATE_TRUNC('minute', DATEADD(minute, -5, CURRENT_TIMESTAMP())) AS interval_start,
      DATE_TRUNC('minute', CURRENT_TIMESTAMP()) AS interval_end,
      CASE WHEN COUNT(1)=0 THEN 1 ELSE SUM(CASE WHEN COALESCE(delay_min,0) <= 5 THEN 1 ELSE 0 END)::FLOAT / COUNT(1) END AS pct_on_time,
      COUNT(1) AS total_count,
      SUM(CASE WHEN COALESCE(delay_min,0) <= 5 THEN 1 ELSE 0 END) AS on_time_count
    FROM GTFS_DB.SILVER.trip_updates_silver
    WHERE insert_date >= DATEADD(minute, -5, CURRENT_TIMESTAMP());
    """

    # 7) travel_time_segments: actual vs scheduled travel times per segment (requires joining stop_times and trip_stops_times)
    create_travel_time_segments_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.travel_time_segments (
      route_id STRING,
      trip_id STRING,
      from_stop_id STRING,
      to_stop_id STRING,
      scheduled_travel_sec INT,
      actual_travel_sec FLOAT,
      sample_count INT,
      avg_delay_sec FLOAT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    # insert_travel_time_segments_sql (Later)

    # 8) delay_distribution: histogram bins counts
    create_delay_distribution_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.delay_distribution (
      bucket_label STRING,
      bucket_min INT,
      bucket_max INT,
      sample_count INT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_delay_distribution_sql = """
    INSERT INTO GTFS_DB.GOLD.delay_distribution (bucket_label, bucket_min, bucket_max, sample_count)
    SELECT bucket_label, bucket_min, bucket_max, COUNT(1) FROM (
      SELECT
        CASE
          WHEN delay_min <= 2 THEN '0-2'
          WHEN delay_min <= 5 THEN '2-5'
          WHEN delay_min <= 10 THEN '5-10'
          WHEN delay_min <= 20 THEN '10-20'
          ELSE '20+' END AS bucket_label,
        CASE
          WHEN delay_min <= 2 THEN 0
          WHEN delay_min <= 5 THEN 2
          WHEN delay_min <= 10 THEN 5
          WHEN delay_min <= 20 THEN 10
          ELSE 20 END AS bucket_min,
        CASE
          WHEN delay_min <= 2 THEN 2
          WHEN delay_min <= 5 THEN 5
          WHEN delay_min <= 10 THEN 10
          WHEN delay_min <= 20 THEN 20
          ELSE 9999 END AS bucket_max
      FROM (
        SELECT COALESCE(delay_min,0) AS delay_min FROM GTFS_DB.SILVER.trip_updates_silver
        UNION ALL
        SELECT COALESCE(tu.delay_min,0) FROM GTFS_DB.SILVER.vehicle_positions_silver vp LEFT JOIN GTFS_DB.SILVER.trip_updates_silver tu ON vp.trip_id = tu.trip_id
      )
    ) GROUP BY bucket_label, bucket_min, bucket_max;
    """

    # 9) top_problem_stops: top N stops by accumulated delay
    create_top_problem_stops_sql = """
    CREATE TABLE IF NOT EXISTS GTFS_DB.GOLD.top_problem_stops (
      stop_id STRING,
      stop_name STRING,
      nb_passages INT,
      avg_delay_min FLOAT,
      total_delay_min FLOAT,
      insert_date TIMESTAMP_NTZ DEFAULT CAST(CONVERT_TIMEZONE('Europe/Paris', CURRENT_TIMESTAMP()) AS TIMESTAMP_NTZ)
    );
    """

    insert_top_problem_stops_sql = """
    INSERT INTO GTFS_DB.GOLD.top_problem_stops (stop_id, stop_name, nb_passages, avg_delay_min, total_delay_min)
    SELECT stop_id, stop_name, nb_passages, avg_delay_min, (nb_passages * avg_delay_min) AS total_delay_min
    FROM GTFS_DB.GOLD.stops_aggregate
    ORDER BY avg_delay_min DESC
    LIMIT 50;
    """

    # -----------------------------------
    # Déclarations des Tasks
    # -----------------------------------
    create_vehicle_positions_latest = SQLExecuteQueryOperator(
        task_id="create_vehicle_positions_latest",
        conn_id="snowflake_conn",
        sql=create_vehicle_positions_latest_sql,
    )
    insert_vehicle_positions_latest = SQLExecuteQueryOperator(
        task_id="insert_vehicle_positions_latest",
        conn_id="snowflake_conn",
        sql=insert_vehicle_positions_latest_sql,
    )

    create_trips_delay_timeseries = SQLExecuteQueryOperator(
        task_id="create_trips_delay_timeseries",
        conn_id="snowflake_conn",
        sql=create_trips_delay_timeseries_sql,
    )
    insert_trips_delay_timeseries = SQLExecuteQueryOperator(
        task_id="insert_trips_delay_timeseries",
        conn_id="snowflake_conn",
        sql=insert_trips_delay_timeseries_sql,
    )

    create_stops_aggregate = SQLExecuteQueryOperator(
        task_id="create_stops_aggregate",
        conn_id="snowflake_conn",
        sql=create_stops_aggregate_sql,
    )
    insert_stops_aggregate = SQLExecuteQueryOperator(
        task_id="insert_stops_aggregate",
        conn_id="snowflake_conn",
        sql=insert_stops_aggregate_sql,
    )

    create_route_delay_agg = SQLExecuteQueryOperator(
        task_id="create_route_delay_agg",
        conn_id="snowflake_conn",
        sql=create_route_delay_agg_sql,
    )
    insert_route_delay_agg = SQLExecuteQueryOperator(
        task_id="insert_route_delay_agg",
        conn_id="snowflake_conn",
        sql=insert_route_delay_agg_sql,
    )

    create_heatmap = SQLExecuteQueryOperator(
        task_id="create_heatmap",
        conn_id="snowflake_conn",
        sql=create_heatmap_sql,
    )
    insert_heatmap = SQLExecuteQueryOperator(
        task_id="insert_heatmap",
        conn_id="snowflake_conn",
        sql=insert_heatmap_sql,
    )

    create_punctuality = SQLExecuteQueryOperator(
        task_id="create_punctuality",
        conn_id="snowflake_conn",
        sql=create_punctuality_sql,
    )
    insert_punctuality = SQLExecuteQueryOperator(
        task_id="insert_punctuality",
        conn_id="snowflake_conn",
        sql=insert_punctuality_sql,
    )

    create_travel_time_segments = SQLExecuteQueryOperator(
        task_id="create_travel_time_segments",
        conn_id="snowflake_conn",
        sql=create_travel_time_segments_sql,
    )
    """ 
    insert_travel_time_segments = SQLExecuteQueryOperator(
        task_id="insert_travel_time_segments",
        conn_id="snowflake_conn",
        sql=insert_travel_time_segments_sql,
    )
    """

    create_delay_distribution = SQLExecuteQueryOperator(
        task_id="create_delay_distribution",
        conn_id="snowflake_conn",
        sql=create_delay_distribution_sql,
    )
    insert_delay_distribution = SQLExecuteQueryOperator(
        task_id="insert_delay_distribution",
        conn_id="snowflake_conn",
        sql=insert_delay_distribution_sql,
    )

    create_top_problem_stops = SQLExecuteQueryOperator(
        task_id="create_top_problem_stops",
        conn_id="snowflake_conn",
        sql=create_top_problem_stops_sql,
    )
    insert_top_problem_stops = SQLExecuteQueryOperator(
        task_id="insert_top_problem_stops",
        conn_id="snowflake_conn",
        sql=insert_top_problem_stops_sql,
    )

    # -----------------------------------
    # Orchestration: create schema -> create tables -> inserts.
    # -----------------------------------
    create_gold_schema >> [
        create_vehicle_positions_latest,
        create_trips_delay_timeseries,
        create_stops_aggregate,
        create_route_delay_agg,
        create_heatmap,
        create_punctuality,
        create_travel_time_segments,
        create_delay_distribution,
        create_top_problem_stops,
    ]

    # RT-focused fast path
    create_vehicle_positions_latest >> insert_vehicle_positions_latest
    create_trips_delay_timeseries >> insert_trips_delay_timeseries
    create_punctuality >> insert_punctuality

    # Aggregations
    create_stops_aggregate >> insert_stops_aggregate
    create_route_delay_agg >> insert_route_delay_agg
    create_heatmap >> insert_heatmap
    create_travel_time_segments # >> insert_travel_time_segments
    create_delay_distribution >> insert_delay_distribution
    create_top_problem_stops >> insert_top_problem_stops

    # Commande facultative pour garantir que les agrégats fonctionnent après les insertions RT (non obligatoire mais juste au cas ou )
    (
        insert_vehicle_positions_latest
        >> insert_trips_delay_timeseries
        >> insert_stops_aggregate
        >> insert_route_delay_agg
        >> insert_heatmap
        >> insert_delay_distribution
        >> insert_top_problem_stops
    )
