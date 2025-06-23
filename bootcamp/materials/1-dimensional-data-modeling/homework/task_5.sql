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
