-- Query 7: Longest streak of games where LeBron James scored over 10 points
WITH games_with_winner AS (
    SELECT
        g.game_id,
        g.game_date_est AS date,
        CASE
            WHEN g.pts_home > g.pts_away THEN g.home_team_id
            WHEN g.pts_home < g.pts_away THEN g.visitor_team_id
            ELSE NULL
        END AS winning_team_id
    FROM games g
),
lebron_games AS (
    SELECT
        gd.game_id,
        gd.pts,
        gw.date,
        CASE
            WHEN gd.pts > 10 THEN 1 ELSE 0 END AS lebron_over_10
    FROM game_details gd
    JOIN games_with_winner gw ON gd.game_id = gw.game_id
    WHERE gd.player_id = (
        SELECT player_id
        FROM game_details
        WHERE player_name ILIKE 'LeBron James'
        LIMIT 1
    )
)
, streaks AS (
    SELECT
        date,
        lebron_over_10,
        SUM(
            CASE
                WHEN lebron_over_10 = 0 THEN 1
                ELSE 0
            END
        ) OVER (ORDER BY date) AS streak_group
    FROM lebron_games
)
, streaks_count as (
    SELECT
        streak_group,
        COUNT(*) AS streak_length
    FROM streaks
    WHERE lebron_over_10 = 1
    GROUP BY streak_group
)

SELECT
    MAX(streak_length) AS max_consecutive_games_over_10
FROM streaks_count; 