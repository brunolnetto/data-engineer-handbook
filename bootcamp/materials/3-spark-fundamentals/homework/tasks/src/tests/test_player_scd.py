from chispa.dataframe_comparer import *
from ..jobs.player_scd_job import do_player_scd_transformation
from collections import namedtuple

# Define named tuples for test data
Player = namedtuple("Player", "player_name current_season scoring_class")
ExpectedSCD = namedtuple("ExpectedSCD", "player_name scoring_class start_date end_date")


def test_player_scd_single_streak(spark):
    """Test player with single scoring class streak"""
    current_season = 2021
    
    # Input data - player stays in same scoring class
    players_data = [
        Player("player1", 2019, "good"),
        Player("player1", 2020, "good"),
        Player("player1", 2021, "good")
    ]
    
    players_df = spark.createDataFrame(players_data)
    actual_df = do_player_scd_transformation(spark, players_df, current_season)
    
    # Expected output - single streak
    expected_data = [
        ExpectedSCD("player1", "good", 2019, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_player_scd_multiple_streaks(spark):
    """Test player with multiple scoring class changes"""
    current_season = 2021
    
    # Input data - player changes scoring class
    players_data = [
        Player("player1", 2019, "average"),
        Player("player1", 2020, "good"),      # Changed from average to good
        Player("player1", 2021, "star")       # Changed from good to star
    ]
    
    players_df = spark.createDataFrame(players_data)
    actual_df = do_player_scd_transformation(spark, players_df, current_season)
    
    # Expected output - three separate streaks
    expected_data = [
        ExpectedSCD("player1", "average", 2019, 2019),
        ExpectedSCD("player1", "good", 2020, 2020),
        ExpectedSCD("player1", "star", 2021, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_player_scd_mixed_scenarios(spark):
    """Test multiple players with different scenarios"""
    current_season = 2021
    
    # Input data - multiple players with different patterns
    players_data = [
        # Player 1: Consistent good performance
        Player("player1", 2019, "good"),
        Player("player1", 2020, "good"),
        Player("player1", 2021, "good"),
        
        # Player 2: Improvement over time
        Player("player2", 2019, "average"),
        Player("player2", 2020, "good"),
        Player("player2", 2021, "star"),
        
        # Player 3: Decline over time
        Player("player3", 2019, "star"),
        Player("player3", 2020, "good"),
        Player("player3", 2021, "average")
    ]
    
    players_df = spark.createDataFrame(players_data)
    actual_df = do_player_scd_transformation(spark, players_df, current_season)
    
    # Expected output
    expected_data = [
        # Player 1: Single streak
        ExpectedSCD("player1", "good", 2019, 2021),
        
        # Player 2: Three streaks (improvement)
        ExpectedSCD("player2", "average", 2019, 2019),
        ExpectedSCD("player2", "good", 2020, 2020),
        ExpectedSCD("player2", "star", 2021, 2021),
        
        # Player 3: Three streaks (decline)
        ExpectedSCD("player3", "star", 2019, 2019),
        ExpectedSCD("player3", "good", 2020, 2020),
        ExpectedSCD("player3", "average", 2021, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_player_scd_with_gaps(spark):
    """Test player with gaps in seasons (missing data)"""
    current_season = 2021
    
    # Input data - player with missing 2020 season
    players_data = [
        Player("player1", 2019, "good"),
        # Missing 2020 season
        Player("player1", 2021, "star")  # Different scoring class
    ]
    
    players_df = spark.createDataFrame(players_data)
    actual_df = do_player_scd_transformation(spark, players_df, current_season)
    
    # Expected output - two separate streaks due to gap
    expected_data = [
        ExpectedSCD("player1", "good", 2019, 2019),
        ExpectedSCD("player1", "star", 2021, 2021)
    ]
    expected_df = spark.createDataFrame(expected_data)
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)


def test_player_scd_empty_input(spark):
    """Test with empty input data"""
    current_season = 2021
    
    # Empty input data
    players_data = []
    players_df = spark.createDataFrame(players_data, "player_name STRING, current_season INT, scoring_class STRING")
    
    actual_df = do_player_scd_transformation(spark, players_df, current_season)
    
    # Expected output - empty DataFrame
    expected_data = []
    expected_df = spark.createDataFrame(expected_data, "player_name STRING, scoring_class STRING, start_date INT, end_date INT")
    
    assert_df_equality(actual_df, expected_df, ignore_row_order=True)