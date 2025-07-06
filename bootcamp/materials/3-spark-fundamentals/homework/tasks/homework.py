#!/usr/bin/env python3
"""
Spark Fundamentals Homework - Requirements Checklist

Business Questions Answered:
1. Which player averages the most kills per game?
2. Which playlist gets played the most?
3. Which map gets played the most?
4. Which map do players get the most Killing Spree medals on?

This script demonstrates Spark best practices:
- Disables automatic broadcast join
- Uses explicit broadcast joins for small tables
- Uses bucket joins for large tables
- Aggregates to answer business questions
- Benchmarks performance and memory usage
- Tests sortWithinPartitions and analyzes compression effects
- Robust error handling and parameterization
"""

import os
import time
import psutil
import sys
from pyspark.sql.utils import AnalysisException

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
# When running in Docker container, data is at /home/iceberg/data
if os.path.exists('/home/iceberg/data'):
    DATA_PATH = '/home/iceberg/data'
else:
    DATA_PATH = os.path.join(SCRIPT_DIR, '../data')

# =====================
# CONFIGURATION SECTION
# =====================
CONFIG = {
    'DATA_PATH': '/home/iceberg/data' if os.path.exists('/home/iceberg/data') else os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data'),
    'BUCKET_COUNT': 16,
    'SMALL_TABLES': ['medals.csv', 'maps.csv'],
    'LARGE_TABLES': ['match_details.csv', 'matches.csv', 'medals_matches_players.csv'],
    'SORT_STRATEGIES': [
        ('playlist_id', 'Sort by playlist_id (low cardinality)'),
        ('mapid', 'Sort by mapid (low cardinality)'),
        ('playlist_id,mapid', 'Sort by playlist_id,mapid'),
        ('mapid,playlist_id', 'Sort by mapid,playlist_id')
    ],
    'SAMPLE_FRACTION': 0.1,
    'SAMPLE_SEED': 42
}

def log_perf(step_name):
    """Utility to log timing and memory usage for a step."""
    mem_mb = psutil.Process().memory_info().rss / 1024 / 1024
    print(f"[PERF] {step_name}: Memory usage = {mem_mb:.2f} MB")

def validate_schema(df, required_cols, df_name):
    """Raise an error if required columns are missing from DataFrame."""
    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")

# =============================================================================
# HOMEWORK REQUIREMENT 1: Disable automatic broadcast join
# =============================================================================
def requirement_1_disable_auto_broadcast():
    """
    Disable automatic broadcast join for all Spark SQL joins.
    Returns a SparkSession with the config set.
    """
    from pyspark.sql import SparkSession
    from pyspark import SparkConf
    conf = SparkConf()
    conf.set("spark.driver.host", "spark-iceberg")
    conf.set("spark.driver.bindAddress", "0.0.0.0")
    conf.set("spark.driver.port", "4040")
    conf.set("spark.blockManager.port", "4041")
    spark = (
        SparkSession.builder
        .appName("Homework")
        .master("local[*]")
        .config("spark.sql.catalog.spark_catalog", "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.spark_catalog.type", "hadoop")
        .config("spark.sql.catalog.spark_catalog.warehouse", "/tmp/iceberg-warehouse")
        .config("spark.sql.catalog.spark_catalog.default-namespace", "default")
        .config("spark.jars.packages", "org.apache.iceberg:iceberg-spark-runtime-3.3_2.12:1.8.1")
        .getOrCreate()
    )
    spark.conf.set("spark.sql.autoBroadcastJoinThreshold", "-1")
    log_perf("Disable auto broadcast join")
    return spark

# =============================================================================
# HOMEWORK REQUIREMENT 2: Explicitly broadcast JOINs medals and maps
# =============================================================================
def requirement_2_broadcast_joins(spark):
    """
    Explicitly broadcast JOINs medals and maps. Returns the fully joined DataFrame.
    """
    from pyspark.sql.functions import broadcast, col
    # Load small lookup tables with schema validation
    medals_path = os.path.join(CONFIG['DATA_PATH'], CONFIG['SMALL_TABLES'][0])
    maps_path = os.path.join(CONFIG['DATA_PATH'], CONFIG['SMALL_TABLES'][1])
    medals = spark.read.csv(medals_path, header=True)
    maps = spark.read.csv(maps_path, header=True)
    validate_schema(medals, ["medal_id", "name", "classification", "description"], "medals")
    validate_schema(maps, ["mapid", "name", "description"], "maps")
    medals = medals.select(col("medal_id"), col("name").alias("medal_name"), col("classification"), col("description").alias("medal_description"))
    maps = maps.select(col("mapid"), col("name").alias("map_name"), col("description").alias("map_description"))
    # Load large tables (bucketed)
    try:
        match_details = spark.table("default.bucketed_match_details")
        matches = spark.table("default.bucketed_matches")
        medals_matches_players = spark.table("default.bucketed_medals_matches_players")
    except AnalysisException as e:
        print("[ERROR] Bucketed tables not found. Did you run the bucket join step?")
        raise e
    # Validate schemas
    validate_schema(match_details, ["match_id", "player_gamertag", "player_total_kills"], "match_details")
    validate_schema(matches, ["match_id", "playlist_id", "mapid"], "matches")
    validate_schema(medals_matches_players, ["match_id", "medal_id", "count"], "medals_matches_players")
    # Join large tables
    t0 = time.perf_counter()
    joined_md_m = match_details.join(matches, "match_id")
    joined_large = joined_md_m.join(medals_matches_players.select("match_id", "medal_id", "count"), "match_id")
    # Broadcast joins
    joined_with_medals = joined_large.join(broadcast(medals), "medal_id", "left")
    final_joined = joined_with_medals.join(broadcast(maps), "mapid", "left")
    t1 = time.perf_counter()
    log_perf("Broadcast joins (medals, maps)")
    print(f"[PERF] Broadcast join time: {t1-t0:.4f} sec")
    return final_joined

# =============================================================================
# HOMEWORK REQUIREMENT 3: Bucket join on match_id with 16 buckets
# =============================================================================
def requirement_3_bucket_joins(spark):
    """
    Bucket join match_details, matches, and medals_matches_players on match_id with configurable buckets.
    """
    bucket_count = CONFIG['BUCKET_COUNT']
    for table_name, file_name in zip(["match_details", "matches", "medals_matches_players"], CONFIG['LARGE_TABLES']):
        temp_view = f"{table_name}_raw"
        bucketed_table = f"default.bucketed_{table_name}"
        file_path = os.path.join(CONFIG['DATA_PATH'], file_name)
        df = spark.read.option("header", "true").csv(file_path)
        if df.rdd.isEmpty():
            raise ValueError(f"[ERROR] {file_name} is empty or missing!")
        df.createOrReplaceTempView(temp_view)
        # Validate schema
        if table_name == "match_details":
            validate_schema(df, ["match_id", "player_gamertag", "player_total_kills"], table_name)
        elif table_name == "matches":
            validate_schema(df, ["match_id", "playlist_id", "mapid"], table_name)
        elif table_name == "medals_matches_players":
            validate_schema(df, ["match_id", "medal_id", "count"], table_name)
        t0 = time.perf_counter()
        spark.sql(f"""
            CREATE OR REPLACE TABLE {bucketed_table}
            USING iceberg
            TBLPROPERTIES ('format-version'='2')
            CLUSTERED BY (match_id) INTO {bucket_count} BUCKETS
            AS SELECT * FROM {temp_view}
        """)
        t1 = time.perf_counter()
        log_perf(f"Bucket join and table creation: {table_name}")
        print(f"[PERF] Bucket join ({table_name}) time: {t1-t0:.4f} sec")

# =============================================================================
# HOMEWORK REQUIREMENT 4: Answer Analytical Questions
# =============================================================================

def question_1_most_kills_per_game(joined_df):
    """
    Which player averages the most kills per game?
    """
    from pyspark.sql.functions import avg, desc, col
    t0 = time.perf_counter()
    result = (joined_df
              .groupBy("player_gamertag")
              .agg(avg("player_total_kills").alias("avg_kills"))
              .orderBy(desc("avg_kills"))
              .limit(5))
    t1 = time.perf_counter()
    log_perf("Analytical Query: Most kills per game")
    print(f"[PERF] Analytical query (most kills per game) time: {t1-t0:.4f} sec")
    return result

def question_2_most_played_playlist(joined_df):
    """
    Which playlist gets played the most?
    """
    from pyspark.sql.functions import count, desc, col
    t0 = time.perf_counter()
    result = (joined_df
              .groupBy("playlist_id")
              .agg(count("*").alias("play_count"))
              .orderBy(desc("play_count"))
              .limit(5))
    t1 = time.perf_counter()
    log_perf("Analytical Query: Most played playlist")
    print(f"[PERF] Analytical query (most played playlist) time: {t1-t0:.4f} sec")
    return result

def question_3_most_played_map(joined_df):
    """
    Which map gets played the most?
    """
    from pyspark.sql.functions import count, desc, col
    t0 = time.perf_counter()
    result = (joined_df
              .groupBy("mapid", "map_name")
              .agg(count("*").alias("map_play_count"))
              .orderBy(desc("map_play_count"))
              .limit(5))
    t1 = time.perf_counter()
    log_perf("Analytical Query: Most played map")
    print(f"[PERF] Analytical query (most played map) time: {t1-t0:.4f} sec")
    return result

def question_4_most_killing_spree_medals_by_map(joined_df):
    """
    Which map do players get the most Killing Spree medals on?
    """
    from pyspark.sql.functions import count, desc, col
    t0 = time.perf_counter()
    result = (joined_df
              .filter(col("medal_name").like("%Killing Spree%"))
              .groupBy("mapid", "map_name")
              .agg(count("*").alias("killing_spree_count"))
              .orderBy(desc("killing_spree_count"))
              .limit(5))
    t1 = time.perf_counter()
    log_perf("Analytical Query: Most Killing Spree medals by map")
    print(f"[PERF] Analytical query (most Killing Spree medals by map) time: {t1-t0:.4f} sec")
    return result

# =============================================================================
# HOMEWORK REQUIREMENT 5: Test sortWithinPartitions
# =============================================================================
def requirement_5_sort_within_partitions(joined_df, spark):
    """
    Try different .sortWithinPartitions to see which has the smallest data size.
    Benchmarks file size and memory usage for each strategy.
    """
    from pyspark.sql.functions import col, countDistinct, count
    print("\n=== FILE SIZE COMPARISON FOR DIFFERENT SORT STRATEGIES ===")
    # Create unsorted table first (using a sample to avoid memory issues)
    print("1. Creating unsorted table...")
    sample_df = joined_df.sample(fraction=CONFIG['SAMPLE_FRACTION'], seed=CONFIG['SAMPLE_SEED'])
    sample_df.write.mode("overwrite").saveAsTable("default.events_unsorted")
    log_perf("Write unsorted sample table")
    # Get unsorted table size
    unsorted_size = spark.sql("""
        SELECT 
            SUM(file_size_in_bytes) as total_size_bytes,
            COUNT(1) as num_files,
            ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0, 2) as total_size_mb
        FROM default.events_unsorted.files
    """).collect()[0]
    print(f"   📊 Unsorted table size: {unsorted_size['total_size_mb']} MB ({unsorted_size['total_size_bytes']} bytes)")
    print(f"   📁 Number of files: {unsorted_size['num_files']}")
    # Analyze data distribution
    print("\n📊 Data Distribution Analysis:")
    sample_stats = sample_df.agg(
        countDistinct("playlist_id").alias("distinct_playlists"),
        countDistinct("mapid").alias("distinct_maps"),
        count("*").alias("total_rows")
    ).collect()[0]
    print(f"   📈 Total rows in sample: {sample_stats['total_rows']:,}")
    print(f"   🎯 Distinct playlists: {sample_stats['distinct_playlists']}")
    print(f"   🗺️  Distinct maps: {sample_stats['distinct_maps']}")
    print(f"   📊 Rows per playlist (avg): {sample_stats['total_rows'] / sample_stats['distinct_playlists']:.0f}")
    print(f"   📊 Rows per map (avg): {sample_stats['total_rows'] / sample_stats['distinct_maps']:.0f}")
    # Create sorted tables
    sort_strategies = CONFIG['SORT_STRATEGIES']
    sort_results = []
    for i, (sort_cols, description) in enumerate(sort_strategies):
        print(f"\n{i+2}. {description}")
        t0 = time.perf_counter()
        if "," in sort_cols:
            cols = [col.strip() for col in sort_cols.split(",")]
            sorted_df = sample_df.sortWithinPartitions(*cols)
        else:
            sorted_df = sample_df.sortWithinPartitions(sort_cols)
        # Write to table
        table_name = f"events_sorted_{i+1}"
        sorted_df.write.mode("overwrite").saveAsTable(f"default.{table_name}")
        log_perf(f"Write sorted table: {description}")
        # Measure file size using Spark SQL
        size_result = spark.sql(f"""
            SELECT 
                SUM(file_size_in_bytes) as total_size_bytes,
                COUNT(1) as num_files,
                ROUND(SUM(file_size_in_bytes) / 1024.0 / 1024.0, 2) as total_size_mb
            FROM default.{table_name}.files
        """).collect()[0]
        # Calculate size difference
        size_diff_mb = size_result['total_size_mb'] - unsorted_size['total_size_mb']
        size_diff_pct = (size_diff_mb / unsorted_size['total_size_mb']) * 100 if unsorted_size['total_size_mb'] else 0
        print(f"   📊 Table size: {size_result['total_size_mb']} MB ({size_result['total_size_bytes']} bytes)")
        print(f"   📁 Number of files: {size_result['num_files']}")
        print(f"   📈 Size difference: {size_diff_mb:+.2f} MB ({size_diff_pct:+.1f}%)")
        t1 = time.perf_counter()
        print(f"[PERF] sortWithinPartitions ({description}) time: {t1-t0:.4f} sec")
        sort_results.append((description, size_result['total_size_mb'], size_result['num_files'], t1-t0))
    print("\n💡 Analysis: Sorted tables may be larger or smaller than unsorted depending on compression, data clustering, and sample size.")
    print("   In production, sorting often helps compression, but effects can vary.")
    return sort_results

# =============================================================================
# COMPLETE HOMEWORK IMPLEMENTATION
# =============================================================================
def complete_homework():
    """
    Run all homework requirements with benchmarking, error handling, and documentation.
    """
    print("=== SPARK FUNDAMENTALS HOMEWORK CHECKLIST ===\n")
    # 1. Disable automatic broadcast join
    print("1. ✅ Disabling automatic broadcast join...")
    spark = requirement_1_disable_auto_broadcast()
    # 2. Create bucketed tables
    print("2. ✅ Creating bucketed tables...")
    requirement_3_bucket_joins(spark)
    # 3. Broadcast joins
    print("3. ✅ Performing broadcast joins...")
    joined_df = requirement_2_broadcast_joins(spark)
    # 4. Analytical questions
    print("4. ✅ Answering analytical questions...")
    print("\n--- Question 1: Most kills per game ---")
    question_1_most_kills_per_game(joined_df).show()
    print("\n--- Question 2: Most played playlist ---")
    question_2_most_played_playlist(joined_df).show()
    print("\n--- Question 3: Most played map ---")
    question_3_most_played_map(joined_df).show()
    print("\n--- Question 4: Most Killing Spree medals by map ---")
    question_4_most_killing_spree_medals_by_map(joined_df).show()
    # 5. sortWithinPartitions
    print("5. ✅ Testing sortWithinPartitions...")
    sort_results = requirement_5_sort_within_partitions(joined_df, spark)
    # Compression summary
    print("\n=== COMPRESSION & FILE SIZE SUMMARY ===")
    print("Sorting within partitions can impact file size due to how Parquet/ORC encodes sorted vs. unsorted data.")
    print("- Sorting by low-cardinality columns (e.g., playlist_id, mapid) can improve compression if data is clustered.")
    print("- However, for small samples or already well-clustered data, sorting may increase file size due to encoding overhead.")
    print("- In production, always benchmark with your real data!")
    print("\nSummary Table:")
    print("Strategy | Size (MB) | # Files | Time (s)")
    for desc, size_mb, num_files, t_sec in sort_results:
        print(f"{desc:35} | {size_mb:8.2f} | {num_files:7} | {t_sec:7.2f}")
    print("\n🎉 ALL HOMEWORK REQUIREMENTS COMPLETED!")
    return joined_df

if __name__ == "__main__":
    complete_homework() 