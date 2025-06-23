-- === Task 2 ===

-- Function to populate the cumulative actor table year-by-year (refactored)
CREATE OR REPLACE FUNCTION populate_actors_cumulative(min_year INTEGER, max_year INTEGER)
RETURNS VOID AS
$$
DECLARE
    subject_year INTEGER;
BEGIN
    FOR subject_year IN SELECT * FROM generate_series(min_year, max_year - 1)
    LOOP
        INSERT INTO actors (actor_id, actor, year, films, quality_class, is_active)
        WITH
            previous_years AS (
                SELECT actor_id, actor, films, quality_class, is_active
                FROM actors
                WHERE year = subject_year
            ),
            current_staging AS (
                SELECT
                    actorid     AS actor_id,
                    actor,
                    subject_year + 1 AS year,
                    ARRAY_AGG(
                        ROW(film, votes, rating, filmid)::film_info
                    ) AS films_current
                FROM actor_films
                WHERE year = subject_year + 1
                GROUP BY actorid, actor
            ),
            current_rating AS (
                SELECT
                    cs.actor_id,
                    AVG(f.rating) AS current_avg_rating
                FROM current_staging cs
                JOIN LATERAL UNNEST(cs.films_current) AS f(film, votes, rating, filmid) ON TRUE
                GROUP BY cs.actor_id
            ),
            merged AS (
                SELECT
                    COALESCE(cs.actor_id, py.actor_id)            AS actor_id,
                    COALESCE(cs.actor, py.actor)                    AS actor,
                    subject_year + 1                               AS year,
                    COALESCE(py.films, ARRAY[]::film_info[]) || cs.films_current AS films,
                    (CASE
                        WHEN cs.actor_id IS NOT NULL AND cr.current_avg_rating > 8 THEN 'star'
                        WHEN cs.actor_id IS NOT NULL AND cr.current_avg_rating > 7 THEN 'good'
                        WHEN cs.actor_id IS NOT NULL AND cr.current_avg_rating > 6 THEN 'average'
                        WHEN cs.actor_id IS NOT NULL                         THEN 'bad'
                        ELSE py.quality_class
                    END)::quality_class                         AS quality_class,
                    (cs.actor_id IS NOT NULL)                       AS is_active
                FROM previous_years py
                FULL OUTER JOIN current_staging cs ON py.actor_id = cs.actor_id
                LEFT JOIN current_rating cr ON cs.actor_id = cr.actor_id
            )
        SELECT m.actor_id, m.actor, m.year, m.films, m.quality_class, m.is_active
        FROM merged m
        WHERE NOT EXISTS (
            SELECT 1 FROM actors a
            WHERE a.actor_id = m.actor_id AND a.year = m.year
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

-- Call the function to populate the table
DO $$
DECLARE
    min_year INTEGER;
    max_year INTEGER;
BEGIN
    SELECT MIN(year), MAX(year) INTO min_year, max_year FROM actor_films;
    PERFORM populate_actors_cumulative(min_year, max_year);
END
$$;