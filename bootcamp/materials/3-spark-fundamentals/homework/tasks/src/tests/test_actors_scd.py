from chispa.dataframe_comparer import *
from ..jobs.actors_scd_job import do_actors_scd_backfill_transformation, do_actors_scd_incremental_transformation
from collections import namedtuple
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, BooleanType, ArrayType, DoubleType

# Define named tuples for test data
Actor = namedtuple("Actor", "actor_id actor year films quality_class is_active")
ActorHistorySCD = namedtuple("ActorHistorySCD", "actor_id actor valid_from valid_to films quality_class is_active")
FilmInfo = namedtuple("FilmInfo", "film votes rating filmid")


def test_actors_scd_backfill_single_streak(spark):
    """Test actor with single quality class streak (no changes)"""
    
    # Input data - actor stays in same quality class
    actors_data = [
        ("actor1", "John Doe", 2020, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor1", "John Doe", 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    
    schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, schema)
    actual_df = do_actors_scd_backfill_transformation(spark, actors_df)
    
    # Expected output - single streak with NULL valid_to (open record)
    expected_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    
    # Match the actual schema (nullable fields)
    expected_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), True),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ]), True), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_backfill_multiple_streaks(spark):
    """Test actor with multiple quality class changes"""
    
    # Input data - actor changes quality class
    actors_data = [
        ("actor1", "John Doe", 2020, [{"film": "Movie1", "votes": 100, "rating": 6.5, "filmid": "f1"}], "average", True),
        ("actor1", "John Doe", 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),      # Changed from average to good
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 9.2, "filmid": "f3"}], "star", True)       # Changed from good to star
    ]
    
    schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, schema)
    actual_df = do_actors_scd_backfill_transformation(spark, actors_df)
    
    # Expected output - three separate streaks, last one with NULL valid_to
    expected_data = [
        ("actor1", "John Doe", 2020, 2020, [{"film": "Movie1", "votes": 100, "rating": 6.5, "filmid": "f1"}], "average", True),
        ("actor1", "John Doe", 2021, 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),
        ("actor1", "John Doe", 2022, None, [{"film": "Movie3", "votes": 200, "rating": 9.2, "filmid": "f3"}], "star", True)
    ]
    
    # Match the actual schema (nullable fields)
    expected_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), True),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ]), True), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_backfill_activity_changes(spark):
    """Test actor with activity status changes"""
    
    # Input data - actor becomes inactive then active again
    actors_data = [
        ("actor1", "John Doe", 2020, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor1", "John Doe", 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", False),     # Became inactive
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)       # Became active again
    ]
    
    schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, schema)
    actual_df = do_actors_scd_backfill_transformation(spark, actors_df)
    
    # Expected output - three streaks due to activity changes, last one with NULL valid_to
    expected_data = [
        ("actor1", "John Doe", 2020, 2020, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor1", "John Doe", 2021, 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", False),
        ("actor1", "John Doe", 2022, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    
    # Match the actual schema (nullable fields)
    expected_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), True),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ]), True), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_backfill_multiple_actors(spark):
    """Test multiple actors with different scenarios"""
    
    # Input data - multiple actors with different patterns
    actors_data = [
        # Actor 1: Consistent good performance
        ("actor1", "John Doe", 2020, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor1", "John Doe", 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True),
        
        # Actor 2: Improvement over time
        ("actor2", "Jane Smith", 2020, [{"film": "Movie4", "votes": 50, "rating": 6.5, "filmid": "f4"}], "average", True),
        ("actor2", "Jane Smith", 2021, [{"film": "Movie5", "votes": 100, "rating": 8.0, "filmid": "f5"}], "good", True),
        ("actor2", "Jane Smith", 2022, [{"film": "Movie6", "votes": 300, "rating": 9.2, "filmid": "f6"}], "star", True),
        
        # Actor 3: Decline over time
        ("actor3", "Bob Johnson", 2020, [{"film": "Movie7", "votes": 200, "rating": 9.0, "filmid": "f7"}], "star", True),
        ("actor3", "Bob Johnson", 2021, [{"film": "Movie8", "votes": 150, "rating": 7.5, "filmid": "f8"}], "good", True),
        ("actor3", "Bob Johnson", 2022, [{"film": "Movie9", "votes": 50, "rating": 5.5, "filmid": "f9"}], "bad", False)
    ]
    
    schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, schema)
    actual_df = do_actors_scd_backfill_transformation(spark, actors_df)
    
    # Expected output - last streaks have NULL valid_to
    expected_data = [
        # Actor 1: Single streak with NULL valid_to
        ("actor1", "John Doe", 2020, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True),
        
        # Actor 2: Three streaks (improvement), last with NULL valid_to
        ("actor2", "Jane Smith", 2020, 2020, [{"film": "Movie4", "votes": 50, "rating": 6.5, "filmid": "f4"}], "average", True),
        ("actor2", "Jane Smith", 2021, 2021, [{"film": "Movie5", "votes": 100, "rating": 8.0, "filmid": "f5"}], "good", True),
        ("actor2", "Jane Smith", 2022, None, [{"film": "Movie6", "votes": 300, "rating": 9.2, "filmid": "f6"}], "star", True),
        
        # Actor 3: Three streaks (decline), last with NULL valid_to
        ("actor3", "Bob Johnson", 2020, 2020, [{"film": "Movie7", "votes": 200, "rating": 9.0, "filmid": "f7"}], "star", True),
        ("actor3", "Bob Johnson", 2021, 2021, [{"film": "Movie8", "votes": 150, "rating": 7.5, "filmid": "f8"}], "good", True),
        ("actor3", "Bob Johnson", 2022, None, [{"film": "Movie9", "votes": 50, "rating": 5.5, "filmid": "f9"}], "bad", False)
    ]
    
    # Match the actual schema (nullable fields)
    expected_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), True),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ]), True), True),
        StructField("quality_class", StringType(), True),
        StructField("is_active", BooleanType(), True)
    ])
    
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_incremental_unchanged_actor(spark):
    """Test incremental SCD for unchanged actor"""
    current_year = 2022
    
    # Current actors data
    actors_data = [
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    
    # Previous SCD history
    history_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True)
    ]
    
    actors_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    history_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), False),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, actors_schema)
    history_df = spark.createDataFrame(history_data, history_schema)
    
    actual_df = do_actors_scd_incremental_transformation(spark, actors_df, history_df, current_year)
    
    # Expected output - extend the open range
    expected_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    expected_df = spark.createDataFrame(expected_data, history_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_incremental_changed_actor(spark):
    """Test incremental SCD for changed actor"""
    current_year = 2022
    
    # Current actors data - quality class changed
    actors_data = [
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 9.2, "filmid": "f3"}], "star", True)
    ]
    
    # Previous SCD history
    history_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True)
    ]
    
    actors_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    history_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), False),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, actors_schema)
    history_df = spark.createDataFrame(history_data, history_schema)
    
    actual_df = do_actors_scd_incremental_transformation(spark, actors_df, history_df, current_year)
    
    # Expected output - close old record and open new one
    expected_data = [
        ("actor1", "John Doe", 2020, 2021, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor1", "John Doe", 2022, None, [{"film": "Movie3", "votes": 200, "rating": 9.2, "filmid": "f3"}], "star", True)
    ]
    expected_df = spark.createDataFrame(expected_data, history_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_incremental_new_actor(spark):
    """Test incremental SCD for new actor"""
    current_year = 2022
    
    # Current actors data - new actor
    actors_data = [
        ("actor2", "Jane Smith", 2022, [{"film": "Movie4", "votes": 300, "rating": 9.0, "filmid": "f4"}], "star", True)
    ]
    
    # Previous SCD history - only actor1
    history_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True)
    ]
    
    actors_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    history_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), False),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, actors_schema)
    history_df = spark.createDataFrame(history_data, history_schema)
    
    actual_df = do_actors_scd_incremental_transformation(spark, actors_df, history_df, current_year)
    
    # Expected output - only the new actor (existing actor is not in current snapshot)
    expected_data = [
        ("actor2", "Jane Smith", 2022, None, [{"film": "Movie4", "votes": 300, "rating": 9.0, "filmid": "f4"}], "star", True)
    ]
    expected_df = spark.createDataFrame(expected_data, history_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_incremental_mixed_scenarios(spark):
    """Test incremental SCD with multiple scenarios"""
    current_year = 2022
    
    # Current actors data
    actors_data = [
        ("actor1", "John Doe", 2022, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True),      # Unchanged
        ("actor2", "Jane Smith", 2022, [{"film": "Movie4", "votes": 300, "rating": 9.2, "filmid": "f4"}], "star", True),    # Changed from good to star
        ("actor3", "Bob Johnson", 2022, [{"film": "Movie5", "votes": 150, "rating": 7.5, "filmid": "f5"}], "good", False)   # Changed activity status
    ]
    
    # Previous SCD history
    history_data = [
        ("actor1", "John Doe", 2020, None, [{"film": "Movie1", "votes": 100, "rating": 8.5, "filmid": "f1"}], "good", True),
        ("actor2", "Jane Smith", 2020, None, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),
        ("actor3", "Bob Johnson", 2020, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True)
    ]
    
    actors_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("year", IntegerType(), False),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    history_schema = StructType([
        StructField("actor_id", StringType(), False),
        StructField("actor", StringType(), False),
        StructField("valid_from", IntegerType(), False),
        StructField("valid_to", IntegerType(), True),
        StructField("films", ArrayType(StructType([
            StructField("film", StringType(), False),
            StructField("votes", IntegerType(), False),
            StructField("rating", DoubleType(), False),
            StructField("filmid", StringType(), False)
        ])), False),
        StructField("quality_class", StringType(), False),
        StructField("is_active", BooleanType(), False)
    ])
    
    actors_df = spark.createDataFrame(actors_data, actors_schema)
    history_df = spark.createDataFrame(history_data, history_schema)
    
    actual_df = do_actors_scd_incremental_transformation(spark, actors_df, history_df, current_year)
    
    # Expected output
    expected_data = [
        # Actor 1: Unchanged - extend open range
        ("actor1", "John Doe", 2020, None, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True),
        
        # Actor 2: Changed quality class - close old, open new
        ("actor2", "Jane Smith", 2020, 2021, [{"film": "Movie2", "votes": 150, "rating": 8.0, "filmid": "f2"}], "good", True),
        ("actor2", "Jane Smith", 2022, None, [{"film": "Movie4", "votes": 300, "rating": 9.2, "filmid": "f4"}], "star", True),
        
        # Actor 3: Changed activity status - close old, open new
        ("actor3", "Bob Johnson", 2020, 2021, [{"film": "Movie3", "votes": 200, "rating": 8.2, "filmid": "f3"}], "good", True),
        ("actor3", "Bob Johnson", 2022, None, [{"film": "Movie5", "votes": 150, "rating": 7.5, "filmid": "f5"}], "good", False)
    ]
    expected_df = spark.createDataFrame(expected_data, history_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_actors_scd_empty_input(spark):
    """Test with empty input data"""
    
    # Empty input data
    actors_data = []
    actors_df = spark.createDataFrame(actors_data, 
        "actor_id STRING, actor STRING, year INT, films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>, quality_class STRING, is_active BOOLEAN")
    
    actual_df = do_actors_scd_backfill_transformation(spark, actors_df)
    
    # Expected output - empty DataFrame
    expected_data = []
    expected_df = spark.createDataFrame(expected_data, 
        "actor_id STRING, actor STRING, valid_from INT, valid_to INT, films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>, quality_class STRING, is_active BOOLEAN")
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True) 