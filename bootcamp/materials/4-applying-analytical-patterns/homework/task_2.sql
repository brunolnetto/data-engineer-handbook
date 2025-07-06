WITH games_with_winner AS (
    SELECT
        g.*,
        CASE
            WHEN g.pts_home > g.pts_away THEN g.home_team_id
            WHEN g.pts_home < g.pts_away THEN g.visitor_team_id
            ELSE NULL -- tie
        END AS winning_team_id
    FROM games g
),
game_details_augmented AS (
    SELECT
        gw.season,
        gw.game_id,
        gd.team_id,
        gd.player_id,
        gd.pts,
        CASE WHEN gd.team_id = gw.winning_team_id THEN 1 ELSE 0 END AS is_win
    FROM
        game_details gd
    JOIN games_with_winner gw ON gw.game_id = gd.game_id
)
SELECT
    coalesce(season::text, '(overall)') as season,
    coalesce(team_id::text, '(overall)') as team_id,
    coalesce(player_id::text, '(overall)') as player_id,
    SUM(pts) AS total_points,
    SUM(is_win) AS total_wins
FROM
    game_details_augmented
GROUP BY GROUPING SETS (
    (team_id, player_id),
    (season, player_id),
    (team_id)
)
order by 1, 2, 3