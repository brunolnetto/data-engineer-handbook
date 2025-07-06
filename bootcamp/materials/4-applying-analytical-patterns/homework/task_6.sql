-- Query 6: Most games a team has won in a rolling 90-game stretch
WITH games_with_winner AS (
    SELECT
        g.game_id,
        g.game_date_est AS date,
        g.home_team_id,
        g.visitor_team_id,
        CASE
            WHEN g.pts_home > g.pts_away THEN g.home_team_id
            WHEN g.pts_home < g.pts_away THEN g.visitor_team_id
            ELSE NULL
        END AS winning_team_id
    FROM games g
),
team_games AS (
    SELECT
        gd.team_id,
        gw.game_id,
        gw.date,
        CASE WHEN gd.team_id = gw.winning_team_id THEN 1 ELSE 0 END AS is_win
    FROM game_details gd
    JOIN games_with_winner gw ON gd.game_id = gw.game_id
),
team_games_ordered AS (
    SELECT
        team_id,
        game_id,
        date,
        is_win,
        ROW_NUMBER() OVER (PARTITION BY team_id ORDER BY date) AS rn
    FROM team_games
)
SELECT
    team_id,
    MAX(wins_in_90_games) AS max_wins_in_90_game_stretch
FROM (
    SELECT
        team_id,
        game_id,
        date,
        SUM(is_win) OVER (
            PARTITION BY team_id
            ORDER BY rn
            ROWS BETWEEN 89 PRECEDING AND CURRENT ROW
        ) AS wins_in_90_games
    FROM team_games_ordered
) t
GROUP BY team_id
ORDER BY max_wins_in_90_game_stretch DESC; 