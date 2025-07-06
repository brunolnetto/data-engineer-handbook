-- Query 4: Player with the most points in a single season
SELECT g.season, gd.player_id, SUM(gd.pts) AS total_points
FROM game_details gd
JOIN games g ON gd.game_id = g.game_id
GROUP BY g.season, gd.player_id
ORDER BY total_points DESC
LIMIT 1; 