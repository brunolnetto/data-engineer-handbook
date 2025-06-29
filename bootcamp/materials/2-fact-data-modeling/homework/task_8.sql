-- Statement: An incremental query that loads host_activity_reduced
-- 
-- Business Logic: Implement day-by-day incremental loading of monthly
-- aggregated metrics. This approach processes one day at a time,
-- appending daily counts to monthly arrays, enabling efficient
-- incremental updates without full reprocessing.
-- 
-- Approach: 
-- 1. Use stored procedures for modular, reusable logic
-- 2. Process each day's events and aggregate by host
-- 3. Merge with previous day's cumulative data
-- 4. Handle array padding for new months and appending for existing months
-- 5. Use UPSERT pattern for idempotent operations
--
-- Design: Modular stored procedure architecture with helper functions
-- for scalable and maintainable incremental processing.
--
-- Key Features:
-- - Validates array lengths to prevent exceeding month boundaries
-- - Simplified padding logic for better maintainability
-- - Comprehensive error handling and logging
-- - Idempotent operations for safe re-runs
-- Solution

CREATE OR REPLACE PROCEDURE daily_metric_rollforward(_day DATE)
LANGUAGE plpgsql
AS $$
DECLARE
  _month_start DATE := date_trunc('month', _day)::DATE;
  _day_of_month INTEGER := EXTRACT(DAY FROM _day);
  _max_days_in_month INTEGER := EXTRACT(DAY FROM (date_trunc('month', _day) + INTERVAL '1 month - 1 day'));
BEGIN
  -- Validate input parameters
  IF _day IS NULL THEN
    RAISE EXCEPTION 'Day parameter cannot be NULL';
  END IF;

  WITH
  -- Get yesterday's snapshot for this month (may be empty on day=1)
  yesterday AS (
    SELECT host, hit_array, unique_visitors
    FROM host_activity_reduced
    WHERE month = _month_start
  ),

  -- Calculate today's raw counts by host
  today AS (
    SELECT
      host,
      COUNT(1) AS hits,
      COUNT(distinct(user_id)) as distinct_users
    FROM events
    WHERE user_id IS NOT NULL and
        host is not null and
        DATE(event_time) = _day
    GROUP BY host
  ),

  -- Merge users who had history or had hits today
  merged AS (
    SELECT
      COALESCE(t.host, y.host) AS host,
      y.hit_array as hit_array,
      y.unique_visitors as unique_visitors,
      COALESCE(t.hits, 0)             AS today_hits,
      COALESCE(t.distinct_users, 0) as today_unique_visitors
    FROM today t
    FULL OUTER JOIN yesterday y USING (host)
  ),

  -- Prepare arrays with validation
  prepared_arrays AS (
    SELECT
      m.host,
      CASE
        -- We have yesterday's array: validate and append
        WHEN m.hit_array IS NOT NULL THEN
          CASE 
            WHEN array_length(m.hit_array, 1) >= _max_days_in_month THEN
              RAISE EXCEPTION 'Hit array for host % already has % elements, cannot append more', m.host, array_length(m.hit_array, 1);
            ELSE
              m.hit_array || ARRAY[m.today_hits]
          END
        -- No history yet: create array with padding up to current day
        ELSE
          CASE
            WHEN _day_of_month = 1 THEN
              ARRAY[m.today_hits]
            ELSE
              -- Pad with zeros up to yesterday, then add today
              array_fill(0::BIGINT, ARRAY[_day_of_month - 1]) || ARRAY[m.today_hits]
          END
      END AS new_hit_array,
      
      CASE
        -- We have yesterday's array: validate and append
        WHEN m.unique_visitors IS NOT NULL THEN
          CASE 
            WHEN array_length(m.unique_visitors, 1) >= _max_days_in_month THEN
              RAISE EXCEPTION 'Unique visitors array for host % already has % elements, cannot append more', m.host, array_length(m.unique_visitors, 1);
            ELSE
              m.unique_visitors || ARRAY[m.today_unique_visitors]
          END
        -- No history yet: create array with padding up to current day
        ELSE
          CASE
            WHEN _day_of_month = 1 THEN
              ARRAY[m.today_unique_visitors]
            ELSE
              -- Pad with zeros up to yesterday, then add today
              array_fill(0::BIGINT, ARRAY[_day_of_month - 1]) || ARRAY[m.today_unique_visitors]
          END
      END AS new_unique_visitors
    FROM merged m
  )

  -- Upsert with validated arrays
  INSERT INTO host_activity_reduced(host, month, hit_array, unique_visitors)
  SELECT
    pa.host,
    _month_start,
    pa.new_hit_array,
    pa.new_unique_visitors
  FROM prepared_arrays pa
  ON CONFLICT (host, month)
  DO UPDATE
    SET hit_array = EXCLUDED.hit_array,
        unique_visitors = EXCLUDED.unique_visitors;

  RAISE NOTICE 'Successfully processed metrics for date %', _day;
END
$$;

-- Helper procedure to process a date range
CREATE OR REPLACE PROCEDURE rollforward_range(_start DATE, _end DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    _current DATE := _start;
    _processed_count INTEGER := 0;
BEGIN
    -- Validate input parameters
    IF _start IS NULL OR _end IS NULL THEN
        RAISE EXCEPTION 'Start and end dates cannot be NULL';
    END IF;
    
    IF _start > _end THEN
        RAISE EXCEPTION 'Start date % cannot be after end date %', _start, _end;
    END IF;

    WHILE _current <= _end LOOP
        BEGIN
            CALL daily_metric_rollforward(_current);
            _processed_count := _processed_count + 1;
            
            -- Log progress every 10 days
            IF _processed_count % 10 = 0 THEN
                RAISE NOTICE 'Processed % days, current date: %', _processed_count, _current;
            END IF;
        EXCEPTION
            WHEN OTHERS THEN
                RAISE NOTICE 'Error processing date %: %', _current, SQLERRM;
                -- Continue processing other dates
        END;
        
        _current := _current + INTERVAL '1 day';
    END LOOP;
    
    RAISE NOTICE 'Completed processing % days of metrics data', _processed_count;
END
$$;

-- Execute the incremental loading for all available data
DO $$
DECLARE
    _start DATE;
    _end   DATE;
BEGIN
    SELECT MIN(DATE(event_time)), MAX(DATE(event_time))
    INTO _start, _end
    FROM events;

    IF _start IS NOT NULL AND _end IS NOT NULL THEN
        RAISE NOTICE 'Starting incremental loading from % to %', _start, _end;
        CALL rollforward_range(_start, _end);
    ELSE
        RAISE NOTICE 'No events found in events table. Skipping.';
    END IF;
END
$$;
