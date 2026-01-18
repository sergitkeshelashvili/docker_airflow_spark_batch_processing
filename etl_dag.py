from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import psycopg2

# Connection for the SOURCE (Default Airflow DB / Public Schema)
SOURCE_CONN = {
    "host": "postgres",
    "port": 5432,
    "database": "airflow",
    "user": "airflow",
    "password": "airflow"
}

# Connection for the DESTINATION (The new Football Research DB)
DEST_CONN = {
    "host": "postgres",
    "port": 5432,
    "database": "football_db",
    "user": "airflow",
    "password": "airflow"
}


def get_spark():
    return (SparkSession.builder
            .appName("PostgresToFootballDB")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.3")
            .config("spark.driver.extraClassPath", "org.postgresql.Driver")
            .getOrCreate())


def setup_databases_and_tables():
    """Initializes the Source data and the Destination DB structure."""
    # 1. Connect to 'airflow' db to ensure source table exists
    conn_src = psycopg2.connect(**SOURCE_CONN)
    conn_src.autocommit = True
    cur_src = conn_src.cursor()

    # Create the source table and add 1 row of dummy data if it's empty
    cur_src.execute("""
                    CREATE TABLE IF NOT EXISTS public.bronze_events
                    (
                        match_id
                        INT,
                        event_type
                        VARCHAR,
                        player_id
                        INT,
                        event_ts
                        TIMESTAMP
                    );
                    INSERT INTO public.bronze_events (match_id, event_type, player_id, event_ts)
                    SELECT 101,
                           'goal',
                           7,
                           NOW() WHERE NOT EXISTS (SELECT 1 FROM public.bronze_events LIMIT 1);
                    """)

    # 2. Create the 'football_db' database
    cur_src.execute("SELECT 1 FROM pg_catalog.pg_database WHERE datname = 'football_db'")
    if not cur_src.fetchone():
        cur_src.execute("CREATE DATABASE football_db")

    cur_src.close()
    conn_src.close()

    # 3. Connect to 'football_db' to create the 'football' schema and tables
    conn_dest = psycopg2.connect(**DEST_CONN)
    cur_dest = conn_dest.cursor()
    cur_dest.execute("CREATE SCHEMA IF NOT EXISTS football;")
    cur_dest.execute("""
                     CREATE TABLE IF NOT EXISTS football.bronze_football_events
                     (
                         match_id
                         INT,
                         event_type
                         VARCHAR,
                         player_id
                         INT,
                         event_ts
                         TIMESTAMP
                     );
                     CREATE TABLE IF NOT EXISTS football.silver_football_events
                     (
                         match_id
                         INT,
                         event_type
                         VARCHAR,
                         player_id
                         INT,
                         event_ts
                         TIMESTAMP
                     );
                     -- Gold table will be created/overwritten by Spark automatically
                     """)
    conn_dest.commit()
    cur_dest.close()
    conn_dest.close()


def spark_etl_logic():
    spark = get_spark()
    src_url = f"jdbc:postgresql://{SOURCE_CONN['host']}:5432/{SOURCE_CONN['database']}"
    dest_url = f"jdbc:postgresql://{DEST_CONN['host']}:5432/{DEST_CONN['database']}"
    jdbc_props = {"user": DEST_CONN["user"], "password": DEST_CONN["password"], "driver": "org.postgresql.Driver"}

    # --- STEP 1: BRONZE (Read from public.bronze_events -> Write to football.bronze_football_events) ---
    df_raw = spark.read.jdbc(src_url, "public.bronze_events", properties=jdbc_props)
    df_raw.write.jdbc(dest_url, "football.bronze_football_events", mode="append", properties=jdbc_props)

    # --- STEP 2: SILVER (Filter/Clean -> Write to football.silver_football_events) ---
    df_silver = df_raw.filter(F.col("event_type").isNotNull()).dropDuplicates()
    df_silver.write.jdbc(dest_url, "football.silver_football_events", mode="overwrite", properties=jdbc_props)

    # --- STEP 3: GOLD (Aggregated Stats -> Write to football.gold_football_stats) ---
    df_gold = df_silver.groupBy("match_id").agg(
        F.count(F.when(F.col("event_type") == 'goal', 1)).alias("total_goals"),
        F.count(F.when(F.col("event_type") == 'foul', 1)).alias("total_fouls")
    )
    df_gold.write.jdbc(dest_url, "football.gold_football_stats", mode="overwrite", properties=jdbc_props)

    spark.stop()


with DAG(
        "football_multi_db_pipeline",
        start_date=datetime(2026, 1, 1),
        schedule_interval=None,
        catchup=False,
        tags=["spark", "medallion", "multi-db"]
) as dag:
    t1 = PythonOperator(task_id="setup_infrastructure", python_callable=setup_databases_and_tables)
    t2 = PythonOperator(task_id="run_spark_medallion_etl", python_callable=spark_etl_logic)

    t1 >> t2
