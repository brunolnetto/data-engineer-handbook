-- Statement: A datelist_int generation query. Convert the device_activity_datelist column into a datelist_int column
-- 
-- Business Logic: Convert date arrays into integer bit masks where each bit position
-- represents a day's activity (1 = active, 0 = inactive). This enables efficient
-- bitwise operations for analyzing user engagement patterns.
-- 
-- Approach: 
-- 1. Generate a 31-day window series (most recent days first)
-- 2. Check if each user was active on each day in the window
-- 3. Create integer where each bit represents a day's activity
-- 4. Most recent day = least significant bit (position 0)
-- 
-- Design: Integer representation allows for fast bitwise operations
-- and compact storage while maintaining daily activity information.
-- Solution:

-- Parameters for the analysis window
WITH params AS (
  SELECT
    DATE '2023-01-31' AS target_day,
    31        AS window_days
),

-- Generate date series for the last N days (most recent first)
series AS (
  SELECT
    p.target_day,
    p.window_days,
    (p.target_day - (n * INTERVAL '1 day'))::date AS series_date,
    n
  FROM params p
  CROSS JOIN generate_series(0, (SELECT window_days - 1 FROM params)) AS n
),

-- Get user activity data from the cumulative table
browser_activity AS (
  SELECT
    u.user_id,
    u.browser_type,
    u.device_activity_datelist AS activity_dates
  FROM user_devices_cumulated u
  WHERE u.date = (SELECT target_day FROM params)
),

-- Mark each day as active (1) or inactive (0) for each user-browser combination
flags AS (
  SELECT
    ba.user_id,
    ba.browser_type,
    s.series_date,
    CASE WHEN ba.activity_dates @> ARRAY[s.series_date] THEN 1 ELSE 0 END AS flag,
    s.n
  FROM browser_activity ba
  CROSS JOIN series s
),

-- Convert to integer representation using bit shifting
datelist_int AS (
  SELECT
    user_id,
    browser_type,
    -- Convert binary flags to integer using bit shifting
    -- Each flag is shifted by its position and OR'd together
    -- Explicitly cast to BIGINT to ensure proper type for bitwise operations
    (SUM(flag::BIGINT << n))::BIGINT AS datelist_int_value
  FROM flags
  GROUP BY user_id, browser_type
)

-- Final result with engagement metrics calculated from the integer
SELECT
  user_id,
  browser_type,
  datelist_int_value,
  -- Count active days by counting set bits
  (SELECT COUNT(*) FROM generate_series(0, 30) AS bit_pos 
   WHERE (datelist_int_value & (1::BIGINT << bit_pos)) > 0) AS days_active_count,
  -- Check if active today (bit 0 is set)
  (datelist_int_value & 1::BIGINT) > 0 AS is_daily_active,
  -- Check if active in last 7 days (any bit 0-6 is set)
  (datelist_int_value & 127::BIGINT) > 0 AS is_weekly_active,
  -- Check if active in last 31 days (any bit is set)
  datelist_int_value > 0 AS is_monthly_active
FROM datelist_int
ORDER BY days_active_count DESC;