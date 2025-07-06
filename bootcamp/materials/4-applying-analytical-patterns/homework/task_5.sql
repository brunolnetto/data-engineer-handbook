-- Query 5: Team with the most total wins
WITH games_with_winner AS (
    SELECT
        g.game_id,
        CASE
            WHEN g.pts_home > g.pts_away THEN g.home_team_id
            WHEN g.pts_home < g.pts_away THEN g.visitor_team_id
            ELSE NULL
        END AS winning_team_id
    FROM games g
)
SELECT gd.team_id, COUNT(*) AS total_wins
FROM game_details gd
JOIN games_with_winner gw ON gd.game_id = gw.game_id
WHERE gd.team_id = gw.winning_team_id
GROUP BY gd.team_id
ORDER BY total_wins DESC
LIMIT 1; 