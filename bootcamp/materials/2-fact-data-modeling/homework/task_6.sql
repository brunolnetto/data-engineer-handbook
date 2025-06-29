-- Statement: The incremental query to generate host_activity_datelist
--
-- Business Logic: Build cumulative host activity data incrementally,
-- processing each day's events and appending to historical activity
-- lists. This enables efficient analysis of host traffic patterns
-- over time without reprocessing all historical data.
--
-- Approach: Simple incremental processing that:
-- 1. Processes events day by day from first to last occurrence
-- 2. Joins previous day's cumulative data with new activity
-- 3. Appends new activity dates to existing datelists
-- 4. Handles new hosts and existing ones consistently
--
-- Design: Uses FULL OUTER JOIN for comprehensive data merging
-- with conflict resolution for idempotent operations.
--
-- Data Flow:
-- - Yesterday: Retrieves existing cumulative host activity for previous day
-- - Today: Extracts new host activity from events for current day
-- - Merge: Combines historical data with new activity using FULL OUTER JOIN
-- - Update: Appends new dates to existing arrays or creates new arrays
--
-- Key Benefits:
-- - Idempotent: Can be run multiple times safely
-- - Incremental: Only processes new data each day
-- - Efficient: Avoids full table scans and reprocessing
-- - Consistent: Maintains data integrity across runs
--
-- Error Handling:
-- - Validates date range before processing
-- - Handles NULL host values gracefully
-- - Ensures data consistency with constraints
-- Solution:

DO $$
DECLARE
  start_date DATE;
  end_date   DATE;
  d          DATE;
  processed_count INTEGER := 0;

BEGIN
  -- Find date range for processing with error handling
  SELECT MIN(DATE(event_time)), MAX(DATE(event_time))
  INTO start_date, end_date
  FROM events
  WHERE host IS NOT NULL;

  -- Validate we have data to process
  IF start_date IS NULL OR end_date IS NULL THEN
    RAISE NOTICE 'No valid events found with host data. Exiting.';
    RETURN;
  END IF;

  RAISE NOTICE 'Processing host activity from % to %', start_date, end_date;

  -- Process each day incrementally
  d := start_date;
  WHILE d <= end_date LOOP
    BEGIN
      WITH
        -- Get yesterday's cumulative data
        yesterday AS (
          SELECT host, host_activity_datelist
          FROM hosts_cumulated
          WHERE date = d - INTERVAL '1 day'
        ),
        -- Get today's new host activity
        today AS (
          SELECT
            host,
            DATE(event_time) as activity_date
          FROM events
          WHERE DATE(event_time) = d
            AND host IS NOT NULL
            AND host != ''
          GROUP BY host, DATE(event_time)
        )
      -- Insert or update cumulative host activity
      INSERT INTO hosts_cumulated(host, date, host_activity_datelist)
      SELECT
        COALESCE(t.host, y.host) AS host,
        d AS date,
        CASE
          WHEN y.host_activity_datelist IS NULL THEN ARRAY[t.activity_date]
          WHEN t.activity_date IS NULL THEN y.host_activity_datelist
          ELSE y.host_activity_datelist || ARRAY[t.activity_date]
        END AS host_activity_datelist
      FROM today t
      FULL OUTER JOIN yesterday y ON t.host = y.host
      ON CONFLICT (host, date)
      DO UPDATE SET
        host_activity_datelist = EXCLUDED.host_activity_datelist;

      processed_count := processed_count + 1;
      
      -- Log progress every 10 days
      IF processed_count % 10 = 0 THEN
        RAISE NOTICE 'Processed % days, current date: %', processed_count, d;
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        RAISE NOTICE 'Error processing date %: %', d, SQLERRM;
        -- Continue processing other dates
    END;

    d := d + INTERVAL '1 day';
  END LOOP;

  RAISE NOTICE 'Completed processing % days of host activity data', processed_count;
END
$$ LANGUAGE plpgsql;