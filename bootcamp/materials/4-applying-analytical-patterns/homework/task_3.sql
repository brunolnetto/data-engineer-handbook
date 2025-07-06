-- Query 3: Player with the most points for a single team
SELECT team_id, player_id, SUM(coalesce(pts, 0)) AS total_points
FROM game_details
GROUP BY team_id, player_id
ORDER BY total_points DESC
LIMIT 1;