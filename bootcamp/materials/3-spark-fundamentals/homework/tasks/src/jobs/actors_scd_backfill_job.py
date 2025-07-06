from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, array, lit, lag, sum as spark_sum, expr, struct, lead, max as spark_max, min as spark_min


def do_actors_scd_backfill_transformation(spark, actors_df):
    """
    Convert PostgreSQL actors_history_scd backfill query to SparkSQL
    
    Original PostgreSQL query (Task 4):
    WITH actor_years AS (
        SELECT
            actor_id,
            actor,
            year,
            films,
            quality_class,
            is_active,
            ROW(quality_class, is_active)::TEXT AS state_vector
        FROM actors
    ),
    change_points AS (
        SELECT
            ay.*,
            (LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) IS DISTINCT FROM state_vector
             OR LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) IS NULL) AS is_new_streak
        FROM actor_years ay
    ),
    streaks AS (
        SELECT
            cp.*,
            SUM(CASE WHEN is_new_streak THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY year) AS streak_id
        FROM change_points cp
    ),
    streak_ranges AS (
        SELECT
            actor_id,
            actor,
            streak_id,
            MIN(year) AS valid_from,
            MAX(year) AS last_year,
            BOOL_OR(is_active) AS is_active,
            MAX(quality_class) AS quality_class
        FROM streaks
        GROUP BY actor_id, actor, streak_id
    ),
    scd_backfill AS (
        SELECT
            sr.actor_id,
            sr.actor,
            sr.valid_from,
            LEAD(sr.valid_from) OVER (PARTITION BY sr.actor_id ORDER BY sr.valid_from) - 1 AS valid_to,
            a2.films AS films,
            sr.quality_class,
            sr.is_active
        FROM streak_ranges sr
        LEFT JOIN LATERAL (
            SELECT films
            FROM actors a2
            WHERE a2.actor_id = sr.actor_id AND a2.year = sr.last_year
            LIMIT 1
        ) a2 ON TRUE
    )
    SELECT actor_id, actor, valid_from, valid_to, films, quality_class, is_active
    FROM scd_backfill
    ORDER BY actor_id, valid_from
    """
    
    # Register DataFrame as temp view
    actors_df.createOrReplaceTempView("actors")
    
    query = """
    WITH actor_years AS (
        SELECT
            actor_id,
            actor,
            year,
            films,
            quality_class,
            is_active,
            -- Create a struct to represent state vector (equivalent to ROW in PostgreSQL)
            struct(quality_class, is_active) AS state_vector
        FROM actors
    ),
    change_points AS (
        SELECT
            ay.*,
            -- Check if state changed from previous row
            (LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) != state_vector
             OR LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) IS NULL) AS is_new_streak
        FROM actor_years ay
    ),
    streaks AS (
        SELECT
            cp.*,
            SUM(CASE WHEN is_new_streak THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY year) AS streak_id
        FROM change_points cp
    ),
    streak_ranges AS (
        SELECT
            actor_id,
            actor,
            streak_id,
            MIN(year) AS valid_from,
            MAX(year) AS last_year,
            -- Use MAX for boolean aggregation (equivalent to BOOL_OR)
            MAX(CAST(is_active AS INT)) = 1 AS is_active,
            MAX(quality_class) AS quality_class
        FROM streaks
        GROUP BY actor_id, actor, streak_id
    ),
    scd_backfill AS (
        SELECT
            sr.actor_id,
            sr.actor,
            sr.valid_from,
            LEAD(sr.valid_from) OVER (PARTITION BY sr.actor_id ORDER BY sr.valid_from) - 1 AS valid_to,
            -- Get films from the last year of the streak
            a2.films AS films,
            sr.quality_class,
            sr.is_active
        FROM streak_ranges sr
        LEFT JOIN (
            SELECT actor_id, year, films
            FROM actors
        ) a2 ON a2.actor_id = sr.actor_id AND a2.year = sr.last_year
    )
    SELECT 
        actor_id, 
        actor, 
        valid_from, 
        valid_to, 
        films, 
        quality_class, 
        is_active
    FROM scd_backfill
    ORDER BY actor_id, valid_from
    """
    
    return spark.sql(query)


def main():
    """
    Main function to demonstrate the actors SCD backfill transformation
    """
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd_backfill") \
        .getOrCreate()
    
    # In a real scenario, this would be loaded from actual data sources
    # For now, we'll use an empty DataFrame as a placeholder
    actors_df = spark.createDataFrame([], 
        "actor_id STRING, actor STRING, year INT, films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>, quality_class STRING, is_active BOOLEAN")
    
    # Run backfill transformation
    backfill_df = do_actors_scd_backfill_transformation(spark, actors_df)
    backfill_df.write.mode("overwrite").insertInto("actors_history_scd_backfill")
    
    print("Actors SCD backfill transformation completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main() 