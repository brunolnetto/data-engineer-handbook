-- Statement: A cumulative query to generate device_activity_datelist from events
-- 
-- Business Logic: Build a cumulative view of user activity by browser type,
-- appending each day's activity to the historical datelist. This enables
-- efficient analysis of user engagement patterns over time.
-- 
-- Approach: Incremental processing that:
-- 1. Processes each day sequentially from first to last event
-- 2. Joins yesterday's cumulative data with today's new activity
-- 3. Appends new activity dates to existing datelists
-- 4. Handles new users/browsers and existing ones consistently
-- 
-- Design: Uses FULL OUTER JOIN to handle both new and existing records,
-- with conflict resolution for idempotent operations.
-- 
-- Data Flow:
-- - Yesterday: Retrieves existing cumulative data for the previous day
-- - Today: Extracts new activity from events table for current day
-- - Merge: Combines historical data with new activity using FULL OUTER JOIN
-- - Update: Appends new dates to existing arrays or creates new arrays
-- 
-- Key Benefits:
-- - Idempotent: Can be run multiple times safely
-- - Incremental: Only processes new data each day
-- - Efficient: Avoids full table scans and reprocessing
-- - Consistent: Maintains data integrity across runs
-- Solution:

DO $$
DECLARE
  start_date DATE;
  end_date   DATE;
  d          DATE;

BEGIN
  -- Find date range for processing
  SELECT MIN(DATE(event_time)), MAX(DATE(event_time))
  INTO start_date, end_date
  FROM events
  WHERE user_id IS NOT NULL;

  -- Process each day incrementally
  d := start_date;
  WHILE d <= end_date LOOP
    WITH
      -- Get yesterday's cumulative data
      yesterday AS (
        SELECT user_id, browser_type, device_activity_datelist
        FROM user_devices_cumulated
        WHERE date = d - INTERVAL '1 day'
      ),
      -- Get today's new activity
      today AS (
        SELECT
          e.user_id,
          d_.browser_type,
          DATE(e.event_time) as activity_date
        FROM events e
        JOIN devices d_ ON e.device_id = d_.device_id
        WHERE DATE(e.event_time) = d
          AND e.user_id IS NOT NULL 
          AND d_.browser_type IS NOT NULL
          AND d_.browser_type != ''
        GROUP BY e.user_id, d_.browser_type, DATE(e.event_time)
      )
    -- Insert or update cumulative data
    INSERT INTO user_devices_cumulated (user_id, date, browser_type, device_activity_datelist)
    SELECT
      COALESCE(t.user_id, y.user_id) AS user_id,
      d AS date,
      COALESCE(t.browser_type, y.browser_type) AS browser_type,
      CASE
        WHEN y.device_activity_datelist IS NULL THEN ARRAY[t.activity_date]
        WHEN t.activity_date IS NULL THEN y.device_activity_datelist
        ELSE y.device_activity_datelist || ARRAY[t.activity_date]
      END AS device_activity_datelist
    FROM today t
    FULL OUTER JOIN yesterday y
      ON t.user_id = y.user_id 
      AND t.browser_type = y.browser_type
    ON CONFLICT (user_id, date, browser_type) 
    DO UPDATE SET 
      device_activity_datelist = EXCLUDED.device_activity_datelist;

    d := d + INTERVAL '1 day';
  END LOOP;
END
$$ LANGUAGE plpgsql;

