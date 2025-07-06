from chispa.dataframe_comparer import *
from ..jobs.user_growth_accounting_job import do_user_growth_accounting_transformation
from collections import namedtuple
from datetime import date, datetime
from pyspark.sql.types import StructType, StructField, StringType, DateType, ArrayType

# Define named tuples for test data
Event = namedtuple("Event", "user_id event_time")
UserGrowthRecord = namedtuple("UserGrowthRecord", "user_id first_active_date last_active_date dates_active date")
ExpectedGrowthRecord = namedtuple("ExpectedGrowthRecord", "user_id first_active_date last_active_date daily_active_state weekly_active_state date_list date")


def test_user_growth_accounting_new_user(spark):
    """Test new user scenario - user appears for the first time"""
    ds = "2023-03-10"
    
    # Input data
    events_data = [
        Event("user1", datetime(2023, 3, 10, 10, 0, 0)),
        Event("user1", datetime(2023, 3, 10, 15, 0, 0))
    ]
    
    # Empty yesterday data (no previous records) - use explicit schema
    yesterday_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_active_date", DateType(), True),
        StructField("last_active_date", DateType(), True),
        StructField("dates_active", ArrayType(DateType()), True),
        StructField("date", DateType(), True)
    ])
    users_growth_accounting_df = spark.createDataFrame([], yesterday_schema)
    
    events_df = spark.createDataFrame(events_data)
    actual_df = do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds)
    
    # Expected output - use explicit schema to match actual output
    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_active_date", StringType(), True),  # SparkSQL returns string for date
        StructField("last_active_date", StringType(), True),
        StructField("daily_active_state", StringType(), False),
        StructField("weekly_active_state", StringType(), False),
        StructField("date_list", ArrayType(StringType()), False),
        StructField("date", StringType(), True)
    ])
    
    expected_data = [
        ExpectedGrowthRecord(
            user_id="user1",
            first_active_date="2023-03-10",
            last_active_date="2023-03-10",
            daily_active_state="New",
            weekly_active_state="New",
            date_list=["2023-03-10"],
            date="2023-03-10"
        )
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_user_growth_accounting_retained_user(spark):
    """Test retained user scenario - user was active yesterday and today"""
    ds = "2023-03-10"
    
    # Input data - user active today
    events_data = [
        Event("user1", datetime(2023, 3, 10, 10, 0, 0))
    ]
    
    # Yesterday data - user was active yesterday
    yesterday_data = [
        UserGrowthRecord(
            user_id="user1",
            first_active_date=date(2023, 3, 5),
            last_active_date=date(2023, 3, 9),  # Yesterday
            dates_active=[date(2023, 3, 5), date(2023, 3, 6), date(2023, 3, 7), date(2023, 3, 8), date(2023, 3, 9)],
            date=date(2023, 3, 9)
        )
    ]
    
    events_df = spark.createDataFrame(events_data)
    users_growth_accounting_df = spark.createDataFrame(yesterday_data, 
        "user_id STRING, first_active_date DATE, last_active_date DATE, dates_active ARRAY<DATE>, date DATE")
    
    actual_df = do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds)
    
    # Expected output - use explicit schema to match actual output
    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_active_date", StringType(), True),
        StructField("last_active_date", StringType(), True),
        StructField("daily_active_state", StringType(), False),
        StructField("weekly_active_state", StringType(), False),
        StructField("date_list", ArrayType(StringType()), False),
        StructField("date", StringType(), True)
    ])
    
    expected_data = [
        ExpectedGrowthRecord(
            user_id="user1",
            first_active_date="2023-03-05",
            last_active_date="2023-03-10",
            daily_active_state="Retained",
            weekly_active_state="Retained",
            date_list=["2023-03-05", "2023-03-06", "2023-03-07", "2023-03-08", "2023-03-09", "2023-03-10"],
            date="2023-03-10"
        )
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_user_growth_accounting_churned_user(spark):
    """Test churned user scenario - user was active yesterday but not today"""
    ds = "2023-03-10"
    
    # Input data - no events today - use explicit schema
    events_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("event_time", StringType(), True)
    ])
    events_df = spark.createDataFrame([], events_schema)
    
    # Yesterday data - user was active yesterday
    yesterday_data = [
        UserGrowthRecord(
            user_id="user1",
            first_active_date=date(2023, 3, 5),
            last_active_date=date(2023, 3, 9),  # Yesterday
            dates_active=[date(2023, 3, 5), date(2023, 3, 6), date(2023, 3, 7), date(2023, 3, 8), date(2023, 3, 9)],
            date=date(2023, 3, 9)
        )
    ]
    
    users_growth_accounting_df = spark.createDataFrame(yesterday_data, 
        "user_id STRING, first_active_date DATE, last_active_date DATE, dates_active ARRAY<DATE>, date DATE")
    
    actual_df = do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds)
    
    # Expected output - use explicit schema to match actual output
    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_active_date", StringType(), True),
        StructField("last_active_date", StringType(), True),
        StructField("daily_active_state", StringType(), False),
        StructField("weekly_active_state", StringType(), False),
        StructField("date_list", ArrayType(StringType()), False),
        StructField("date", StringType(), True)
    ])
    
    expected_data = [
        ExpectedGrowthRecord(
            user_id="user1",
            first_active_date="2023-03-05",
            last_active_date="2023-03-09",
            daily_active_state="Churned",
            weekly_active_state="Stale",  # Not churned weekly yet
            date_list=["2023-03-05", "2023-03-06", "2023-03-07", "2023-03-08", "2023-03-09"],
            date="2023-03-10"
        )
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_user_growth_accounting_resurrected_user(spark):
    """Test resurrected user scenario - user was inactive but came back"""
    ds = "2023-03-10"
    
    # Input data - user active today
    events_data = [
        Event("user1", datetime(2023, 3, 10, 10, 0, 0))
    ]
    
    # Yesterday data - user was last active 3 days ago
    yesterday_data = [
        UserGrowthRecord(
            user_id="user1",
            first_active_date=date(2023, 3, 1),
            last_active_date=date(2023, 3, 7),  # 3 days ago
            dates_active=[date(2023, 3, 1), date(2023, 3, 2), date(2023, 3, 3), date(2023, 3, 4), date(2023, 3, 5), date(2023, 3, 6), date(2023, 3, 7)],
            date=date(2023, 3, 9)
        )
    ]
    
    events_df = spark.createDataFrame(events_data)
    users_growth_accounting_df = spark.createDataFrame(yesterday_data, 
        "user_id STRING, first_active_date DATE, last_active_date DATE, dates_active ARRAY<DATE>, date DATE")
    
    actual_df = do_user_growth_accounting_transformation(spark, events_df, users_growth_accounting_df, ds)
    
    # Expected output - use explicit schema to match actual output
    expected_schema = StructType([
        StructField("user_id", StringType(), True),
        StructField("first_active_date", StringType(), True),
        StructField("last_active_date", StringType(), True),
        StructField("daily_active_state", StringType(), False),
        StructField("weekly_active_state", StringType(), False),
        StructField("date_list", ArrayType(StringType()), False),
        StructField("date", StringType(), True)
    ])
    
    expected_data = [
        ExpectedGrowthRecord(
            user_id="user1",
            first_active_date="2023-03-01",
            last_active_date="2023-03-10",
            daily_active_state="Resurrected",
            weekly_active_state="Resurrected",
            date_list=["2023-03-01", "2023-03-02", "2023-03-03", "2023-03-04", "2023-03-05", "2023-03-06", "2023-03-07", "2023-03-10"],
            date="2023-03-10"
        )
    ]
    expected_df = spark.createDataFrame(expected_data, expected_schema)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True) 