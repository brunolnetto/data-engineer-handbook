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