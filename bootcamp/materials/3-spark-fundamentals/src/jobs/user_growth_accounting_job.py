from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, array, lit, date_add, datediff, expr


def do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds):
    """
    Convert PostgreSQL user growth accounting query to SparkSQL
    
    Original PostgreSQL query:
    WITH yesterday AS (
        SELECT * FROM users_growth_accounting
        WHERE date = DATE('2023-03-09')
    ),
    today AS (
        SELECT
           CAST(user_id AS TEXT) as user_id,
           DATE_TRUNC('day', event_time::timestamp) as today_date,
           COUNT(1)
        FROM events
        WHERE DATE_TRUNC('day', event_time::timestamp) = DATE('2023-03-10')
        AND user_id IS NOT NULL
        GROUP BY user_id, DATE_TRUNC('day', event_time::timestamp)
    )
    SELECT COALESCE(t.user_id, y.user_id) as user_id,
           COALESCE(y.first_active_date, t.today_date) AS first_active_date,
           COALESCE(t.today_date, y.last_active_date) AS last_active_date,
           CASE
               WHEN y.user_id IS NULL THEN 'New'
               WHEN y.last_active_date = t.today_date - Interval '1 day' THEN 'Retained'
               WHEN y.last_active_date < t.today_date - Interval '1 day' THEN 'Resurrected'
               WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
               ELSE 'Stale'
           END as daily_active_state,
           CASE
               WHEN y.user_id IS NULL THEN 'New'
               WHEN y.last_active_date < t.today_date - Interval '7 day' THEN 'Resurrected'
               WHEN t.today_date IS NULL AND y.last_active_date = y.date - interval '7 day' THEN 'Churned'
               WHEN COALESCE(t.today_date, y.last_active_date) + INTERVAL '7 day' >= y.date THEN 'Retained'
               ELSE 'Stale'
           END as weekly_active_state,
           COALESCE(y.dates_active, ARRAY[]::DATE[]) || 
           CASE WHEN t.user_id IS NOT NULL THEN ARRAY[t.today_date] ELSE ARRAY[]::DATE[] END AS date_list,
           COALESCE(t.today_date, y.date + Interval '1 day') as date
    FROM today t
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
    """
    
    # Register DataFrames as temp views
    events_df.createOrReplaceTempView("events")
    users_growth_accounting_df.createOrReplaceTempView("users_growth_accounting")
    
    # Calculate yesterday's date (ds - 1 day)
    yesterday_date = spark.sql(f"SELECT date_add('{ds}', -1) as yesterday_date").collect()[0]['yesterday_date']
    
    query = f"""
    WITH yesterday AS (
        SELECT * FROM users_growth_accounting
        WHERE date = '{yesterday_date}'
    ),
    today AS (
        SELECT
           CAST(user_id AS STRING) as user_id,
           date_trunc('day', event_time) as today_date,
           COUNT(1) as event_count
        FROM events
        WHERE date_trunc('day', event_time) = '{ds}'
        AND user_id IS NOT NULL
        GROUP BY user_id, date_trunc('day', event_time)
    )
    SELECT 
           COALESCE(t.user_id, y.user_id) as user_id,
           date_format(COALESCE(y.first_active_date, t.today_date), 'yyyy-MM-dd') AS first_active_date,
           date_format(COALESCE(t.today_date, y.last_active_date), 'yyyy-MM-dd') AS last_active_date,
           CASE
               WHEN y.user_id IS NULL THEN 'New'
               WHEN y.last_active_date = date_add(t.today_date, -1) THEN 'Retained'
               WHEN y.last_active_date < date_add(t.today_date, -1) THEN 'Resurrected'
               WHEN t.today_date IS NULL AND y.last_active_date = y.date THEN 'Churned'
               ELSE 'Stale'
           END as daily_active_state,
           CASE
               WHEN y.user_id IS NULL THEN 'New'
               WHEN y.last_active_date < date_add(t.today_date, -1) THEN 'Resurrected'
               WHEN t.today_date IS NULL AND y.last_active_date = date_add('{ds}', -7) THEN 'Churned'
               WHEN t.today_date IS NOT NULL AND COALESCE(t.today_date, y.last_active_date) >= date_add('{ds}', -7) THEN 'Retained'
               ELSE 'Stale'
           END as weekly_active_state,
           TRANSFORM(
               COALESCE(y.dates_active, array()) || 
               CASE WHEN t.user_id IS NOT NULL THEN array(t.today_date) ELSE array() END,
               x -> date_format(x, 'yyyy-MM-dd')
           ) AS date_list,
           date_format(COALESCE(t.today_date, date_add(y.date, 1)), 'yyyy-MM-dd') as date
    FROM today t
    FULL OUTER JOIN yesterday y ON t.user_id = y.user_id
    """
    
    return spark.sql(query)


def main():
    ds = '2023-03-10'
    spark = SparkSession.builder \
        .master("local") \
        .appName("user_growth_accounting") \
        .getOrCreate()
    
    # In a real scenario, these would be loaded from actual data sources
    # For now, we'll use empty DataFrames as placeholders
    events_df = spark.createDataFrame([], "user_id STRING, event_time TIMESTAMP")
    users_growth_accounting_df = spark.createDataFrame([], 
        "user_id STRING, first_active_date DATE, last_active_date DATE, dates_active ARRAY<DATE>, date DATE")
    
    output_df = do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds)
    output_df.write.mode("overwrite").insertInto("user_growth_accounting") 