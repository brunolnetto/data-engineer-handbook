-- === Task 1 ===

-- Drop existing objects
DROP TABLE IF EXISTS actors CASCADE;
DROP TYPE IF EXISTS quality_class CASCADE;
DROP TYPE IF EXISTS film_info CASCADE;
DROP FUNCTION IF EXISTS populate_actors_cumulative;

-- Enum type for classification
CREATE TYPE quality_class AS ENUM ('star', 'good', 'average', 'bad');

-- Composite type for films (adjusted filmid to TEXT to match source)
CREATE TYPE film_info AS (
  film   TEXT,
  votes  INTEGER,
  rating NUMERIC(3,1),
  filmid TEXT
);

-- Table definition for actors (Task 1)
CREATE TABLE actors (
    actor_id TEXT,
    actor TEXT,
    year INTEGER,
    films film_info[],
    quality_class quality_class,
    is_active BOOLEAN
);

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

-- === Task 3: DDL for actors_history_scd ===
-- Drop previous table if exists
DROP TABLE IF EXISTS actors_history_scd CASCADE;

-- Create the SCD Type 2 history table
CREATE TABLE actors_history_scd (
    actor_id     TEXT,
    actor        TEXT,
    valid_from   INTEGER,
    valid_to     INTEGER,
    films        film_info[],
    quality_class quality_class,
    is_active    BOOLEAN,
    PRIMARY KEY(actor_id, valid_from)
);

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

-- === Task 5: Incremental SCD Type 2 merge ===
-- Composite SCD type for unnesting changed records
CREATE TYPE actor_scd_type AS (
    actor_id      TEXT,
    actor         TEXT,
    valid_from    INTEGER,
    valid_to      INTEGER,
    films         film_info[],
    quality_class quality_class,
    is_active     BOOLEAN
);

-- Prepare current and previous states
WITH
last_open AS (
    SELECT *
    FROM actors_history_scd
    WHERE valid_to IS NULL
),
current_snapshot AS (
    SELECT * FROM actors WHERE year = (SELECT MAX(year) FROM actors)
),
-- Unchanged actors: extend open ranges
unchanged AS (
    SELECT
        cs.actor_id,
        cs.actor,
        lo.valid_from,
        NULL::INTEGER       AS valid_to,
        cs.films,
        cs.quality_class,
        cs.is_active
    FROM current_snapshot cs
    JOIN last_open lo USING(actor_id)
    WHERE cs.quality_class = lo.quality_class
      AND cs.is_active     = lo.is_active
),
-- Changed actors: close old and open new
changed AS (
    SELECT
        UNNEST(ARRAY[
            ROW(lo.actor_id, lo.actor, lo.valid_from, cs.year - 1, lo.films, lo.quality_class, lo.is_active)::actor_scd_type,
            ROW(cs.actor_id, cs.actor, cs.year,     NULL,   cs.films, cs.quality_class, cs.is_active)::actor_scd_type
        ]) AS rec
    FROM current_snapshot cs
    JOIN last_open lo USING(actor_id)
    WHERE cs.quality_class IS DISTINCT FROM lo.quality_class
       OR cs.is_active     IS DISTINCT FROM lo.is_active
),
unnested_changed AS (
    SELECT
        (rec).actor_id,
        (rec).actor,
        (rec).valid_from,
        (rec).valid_to,
        (rec).films,
        (rec).quality_class,
        (rec).is_active
    FROM changed
),
-- New actors: never seen before
new_actors AS (
    SELECT
        cs.actor_id,
        cs.actor,
        cs.year       AS valid_from,
        NULL::INTEGER AS valid_to,
        cs.films,
        cs.quality_class,
        cs.is_active
    FROM current_snapshot cs
    LEFT JOIN last_open lo USING(actor_id)
    WHERE lo.actor_id IS NULL
),
incremental_scd AS (
    SELECT * FROM unchanged
    UNION ALL
    SELECT * FROM unnested_changed
    UNION ALL
    SELECT * FROM new_actors
),
-- Actors to close (disappeared or changed)
to_close AS (
    SELECT lo.actor_id, (SELECT MAX(year) FROM actors) AS close_to
    FROM last_open lo
    LEFT JOIN current_snapshot cs USING(actor_id)
    WHERE cs.actor_id IS NULL
       OR cs.quality_class IS DISTINCT FROM lo.quality_class
       OR cs.is_active     IS DISTINCT FROM lo.is_active
)
-- Close old open records
UPDATE actors_history_scd h
SET valid_to = tc.close_to
FROM to_close tc
WHERE h.actor_id = tc.actor_id
  AND h.valid_to IS NULL;
