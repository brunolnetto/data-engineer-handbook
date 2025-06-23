
-- === Task 4: Full-refresh backfill query ===
-- Populate actors_history_scd from scratch
WITH actor_years AS (
    SELECT
        actor_id,
        actor,
        year,
        films,
        quality_class,
        is_active,
        ROW(quality_class, is_active)::TEXT AS state_vector
    FROM actors
),
change_points AS (
    SELECT
        ay.*,
        (LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) IS DISTINCT FROM state_vector
         OR LAG(state_vector) OVER (PARTITION BY actor_id ORDER BY year) IS NULL) AS is_new_streak
    FROM actor_years ay
),
streaks AS (
    SELECT
        cp.*,
        SUM(CASE WHEN is_new_streak THEN 1 ELSE 0 END) OVER (PARTITION BY actor_id ORDER BY year) AS streak_id
    FROM change_points cp
),
streak_ranges AS (
    SELECT
        actor_id,
        actor,
        streak_id,
        MIN(year) AS valid_from,
        MAX(year) AS last_year,
        BOOL_OR(is_active) AS is_active,
        MAX(quality_class) AS quality_class
    FROM streaks
    GROUP BY actor_id, actor, streak_id
),
scd_backfill AS (
    SELECT
        sr.actor_id,
        sr.actor,
        sr.valid_from,
        LEAD(sr.valid_from) OVER (PARTITION BY sr.actor_id ORDER BY sr.valid_from) - 1 AS valid_to,
        -- grab films from last_year
        a2.films AS films,
        sr.quality_class,
        sr.is_active
    FROM streak_ranges sr
    LEFT JOIN LATERAL (
        SELECT films
        FROM actors a2
        WHERE a2.actor_id = sr.actor_id AND a2.year = sr.last_year
        LIMIT 1
    ) a2 ON TRUE
)
INSERT INTO actors_history_scd (actor_id, actor, valid_from, valid_to, films, quality_class, is_active)
SELECT actor_id, actor, valid_from, valid_to, films, quality_class, is_active
FROM scd_backfill
ORDER BY actor_id, valid_from;