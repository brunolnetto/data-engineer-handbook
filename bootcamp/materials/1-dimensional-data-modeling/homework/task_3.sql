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
