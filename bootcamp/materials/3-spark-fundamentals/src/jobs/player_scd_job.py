from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, coalesce, array, lit, lag, sum as spark_sum, expr


def do_player_scd_transformation(spark, players_df, current_season):
    """
    Convert PostgreSQL player SCD query to SparkSQL
    
    Original PostgreSQL query:
    WITH streak_started AS (
        SELECT player_name,
               current_season,
               scoring_class,
               LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) <> scoring_class OR 
               LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL AS did_change
        FROM players
    ),
    streak_identified AS (
        SELECT
            player_name,
            scoring_class,
            current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
        FROM streak_started
    ),
    aggregated AS (
        SELECT
            player_name,
            scoring_class,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date
        FROM streak_identified
        GROUP BY 1,2,3
    )
    SELECT player_name, scoring_class, start_date, end_date
    FROM aggregated
    """
    
    # Register DataFrame as temp view
    players_df.createOrReplaceTempView("players")
    
    query = f"""
    WITH streak_started AS (
        SELECT player_name,
               current_season,
               scoring_class,
               (LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) != scoring_class OR 
                LAG(scoring_class, 1) OVER (PARTITION BY player_name ORDER BY current_season) IS NULL) AS did_change
        FROM players
        WHERE current_season <= {current_season}
    ),
    streak_identified AS (
        SELECT
            player_name,
            scoring_class,
            current_season,
            SUM(CASE WHEN did_change THEN 1 ELSE 0 END)
                OVER (PARTITION BY player_name ORDER BY current_season) as streak_identifier
        FROM streak_started
    ),
    aggregated AS (
        SELECT
            player_name,
            scoring_class,
            streak_identifier,
            MIN(current_season) AS start_date,
            MAX(current_season) AS end_date
        FROM streak_identified
        GROUP BY player_name, scoring_class, streak_identifier
    )
    SELECT player_name, scoring_class, start_date, end_date
    FROM aggregated
    ORDER BY player_name, start_date
    """
    
    return spark.sql(query)


def main():
    current_season = 2021
    spark = SparkSession.builder \
        .master("local") \
        .appName("player_scd") \
        .getOrCreate()
    
    # In a real scenario, this would be loaded from actual data sources
    # For now, we'll use empty DataFrame as placeholder
    players_df = spark.createDataFrame([], 
        "player_name STRING, current_season INT, scoring_class STRING")
    
    output_df = do_player_scd_transformation(spark, players_df, current_season)
    output_df.write.mode("overwrite").insertInto("players_scd") 