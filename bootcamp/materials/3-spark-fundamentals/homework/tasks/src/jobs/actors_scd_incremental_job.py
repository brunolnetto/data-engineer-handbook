from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, array, lit, lag, sum as spark_sum, expr, struct, lead, max as spark_max, min as spark_min


def do_actors_scd_incremental_transformation(spark, actors_df, actors_history_scd_df, current_year):
    """
    Convert PostgreSQL actors_history_scd incremental query to SparkSQL
    
    Original PostgreSQL query (Task 5):
    WITH
    last_open AS (
        SELECT *
        FROM actors_history_scd
        WHERE valid_to IS NULL
    ),
    current_snapshot AS (
        SELECT * FROM actors WHERE year = (SELECT MAX(year) FROM actors)
    ),
    unchanged AS (
        SELECT
            cs.actor_id,
            cs.actor,
            lo.valid_from,
            NULL::INTEGER AS valid_to,
            cs.films,
            cs.quality_class,
            cs.is_active
        FROM current_snapshot cs
        JOIN last_open lo USING(actor_id)
        WHERE cs.quality_class = lo.quality_class
          AND cs.is_active = lo.is_active
    ),
    changed AS (
        SELECT
            UNNEST(ARRAY[
                ROW(lo.actor_id, lo.actor, lo.valid_from, cs.year - 1, lo.films, lo.quality_class, lo.is_active)::actor_scd_type,
                ROW(cs.actor_id, cs.actor, cs.year, NULL, cs.films, cs.quality_class, cs.is_active)::actor_scd_type
            ]) AS rec
        FROM current_snapshot cs
        JOIN last_open lo USING(actor_id)
        WHERE cs.quality_class IS DISTINCT FROM lo.quality_class
           OR cs.is_active IS DISTINCT FROM lo.is_active
    ),
    new_actors AS (
        SELECT
            cs.actor_id,
            cs.actor,
            cs.year AS valid_from,
            NULL::INTEGER AS valid_to,
            cs.films,
            cs.quality_class,
            cs.is_active
        FROM current_snapshot cs
        LEFT JOIN last_open lo USING(actor_id)
        WHERE lo.actor_id IS NULL
    )
    SELECT * FROM unchanged
    UNION ALL
    SELECT * FROM unnested_changed
    UNION ALL
    SELECT * FROM new_actors
    """
    
    # Register DataFrames as temp views
    actors_df.createOrReplaceTempView("actors")
    actors_history_scd_df.createOrReplaceTempView("actors_history_scd")
    
    query = f"""
    WITH
    last_open AS (
        SELECT *
        FROM actors_history_scd
        WHERE valid_to IS NULL
    ),
    current_snapshot AS (
        SELECT * FROM actors WHERE year = {current_year}
    ),
    unchanged AS (
        SELECT
            cs.actor_id,
            cs.actor,
            lo.valid_from,
            NULL AS valid_to,
            cs.films,
            cs.quality_class,
            cs.is_active
        FROM current_snapshot cs
        JOIN last_open lo ON cs.actor_id = lo.actor_id
        WHERE cs.quality_class = lo.quality_class
          AND cs.is_active = lo.is_active
    ),
    changed_old AS (
        SELECT
            lo.actor_id,
            lo.actor,
            lo.valid_from,
            {current_year} - 1 AS valid_to,
            lo.films,
            lo.quality_class,
            lo.is_active
        FROM current_snapshot cs
        JOIN last_open lo ON cs.actor_id = lo.actor_id
        WHERE cs.quality_class != lo.quality_class
           OR cs.is_active != lo.is_active
    ),
    changed_new AS (
        SELECT
            cs.actor_id,
            cs.actor,
            {current_year} AS valid_from,
            NULL AS valid_to,
            cs.films,
            cs.quality_class,
            cs.is_active
        FROM current_snapshot cs
        JOIN last_open lo ON cs.actor_id = lo.actor_id
        WHERE cs.quality_class != lo.quality_class
           OR cs.is_active != lo.is_active
    ),
    new_actors AS (
        SELECT
            cs.actor_id,
            cs.actor,
            {current_year} AS valid_from,
            NULL AS valid_to,
            cs.films,
            cs.quality_class,
            cs.is_active
        FROM current_snapshot cs
        LEFT JOIN last_open lo ON cs.actor_id = lo.actor_id
        WHERE lo.actor_id IS NULL
    )
    SELECT * FROM unchanged
    UNION ALL
    SELECT * FROM changed_old
    UNION ALL
    SELECT * FROM changed_new
    UNION ALL
    SELECT * FROM new_actors
    ORDER BY actor_id, valid_from
    """
    
    return spark.sql(query)


def main():
    """
    Main function to demonstrate the actors SCD incremental transformation
    """
    current_year = 2022
    spark = SparkSession.builder \
        .master("local") \
        .appName("actors_scd_incremental") \
        .getOrCreate()
    
    # In a real scenario, these would be loaded from actual data sources
    # For now, we'll use empty DataFrames as placeholders
    actors_df = spark.createDataFrame([], 
        "actor_id STRING, actor STRING, year INT, films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>, quality_class STRING, is_active BOOLEAN")
    
    actors_history_scd_df = spark.createDataFrame([], 
        "actor_id STRING, actor STRING, valid_from INT, valid_to INT, films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>, quality_class STRING, is_active BOOLEAN")
    
    # Run incremental transformation
    incremental_df = do_actors_scd_incremental_transformation(spark, actors_df, actors_history_scd_df, current_year)
    incremental_df.write.mode("overwrite").insertInto("actors_history_scd_incremental")
    
    print("Actors SCD incremental transformation completed successfully!")
    spark.stop()


if __name__ == "__main__":
    main() 