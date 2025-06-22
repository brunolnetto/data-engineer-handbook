-- Goal: **Cumulative table generation query:** 
-- Statement: Write a query that populates the `actors` table one year at a time. 
CREATE OR REPLACE FUNCTION populate_actors_cumulative(from_year integer, to_year integer)
RETURNS void AS
$$
DECLARE
    subject_year integer;
BEGIN
    FOR subject_year IN SELECT * FROM generate_series(from_year, to_year - 1)
    LOOP
        INSERT INTO actors (actorid, actor, year, films, quality_class, is_active)
        WITH
            previous_year_cte AS (
                SELECT subject_year AS subject_year
            ),
            previous_years AS (
                SELECT *
                FROM actors
                CROSS JOIN previous_year_cte
                WHERE year = previous_year_cte.subject_year
            ),
            current_staging AS (
                SELECT
                    actorid AS actor_id,
                    actor,
                    year,
                    COALESCE(
                        json_agg(
                            json_build_object(
                                'filmid', filmid,
                                'film', film,
                                'votes', votes,
                                'rating', rating
                            )
                        )::jsonb, '[]'::jsonb
                    ) AS films_current
                FROM actor_films
                CROSS JOIN previous_year_cte
                WHERE year = previous_year_cte.subject_year + 1
                GROUP BY 1, 2, 3
            ),
            current_rating AS (
                SELECT
                    c.actor_id,
                    AVG((film->>'rating')::numeric) AS current_avg_rating
                FROM
                    current_staging c
                LEFT JOIN previous_years p
                    ON c.actor = p.actor AND c.year = p.year + 1,
                LATERAL jsonb_array_elements(
                    COALESCE(p.films, '[]'::jsonb) || c.films_current
                ) AS film
                GROUP BY c.actor_id
            ),
            current_year AS (
                SELECT
                    cs.actor_id,
                    cs.actor,
                    cs.year,
                    cs.films_current AS films,
                    (
                        CASE
                            WHEN cr.current_avg_rating > 8 THEN 'star'
                            WHEN cr.current_avg_rating > 7 THEN 'good'
                            WHEN cr.current_avg_rating > 6 THEN 'average'
                            ELSE 'bad'
                        END
                    )::quality_class AS quality_class,
                    pg_column_size(cs.films_current) <> 0 AS is_active
                FROM current_staging cs
                JOIN current_rating cr ON cs.actor_id = cr.actor_id
            )
        SELECT *
        FROM current_year cy
        WHERE NOT EXISTS (
            SELECT 1
            FROM actors a
            WHERE a.actorid = cy.actor_id AND a.year = cy.year
        );
    END LOOP;
END;
$$ LANGUAGE plpgsql;

DO $$
DECLARE
    min_year integer;
    max_year integer;
BEGIN
    SELECT min(year), max(year) INTO min_year, max_year FROM actor_films;

    PERFORM populate_actors_cumulative(min_year, max_year);
END
$$;