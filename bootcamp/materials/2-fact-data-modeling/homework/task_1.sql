-- Statement: A query to deduplicate game_details from Day 1 so there's no duplicates
-- 
-- Business Logic: Remove duplicate game_details records based on the natural key combination
-- of game_id, team_id, and player_id. We keep the first occurrence when multiple records
-- have the same key values.
-- 
-- Approach: Use DISTINCT ON to select unique combinations, ensuring consistent
-- results across runs by maintaining the original table structure from Day 1.
-- Solution:
SELECT DISTINCT ON (game_id, team_id, player_id) *
FROM game_details;

