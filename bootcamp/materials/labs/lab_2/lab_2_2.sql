drop table if exists users_cumulated;
create table if not exists users_cumulated (
    user_id TEXT,
    date DATE, -- The current date for the user
    dates_active DATE[], -- The list of dates in the past where the user was active
    PRIMARY KEY(USER_ID, date)
);


DO $$
DECLARE
  start_date DATE;
  end_date   DATE;
  d          DATE;

BEGIN
  -- 1) find the overall bounds
  SELECT
    MIN(DATE(event_time)),
    MAX(DATE(event_time))
  INTO start_date, end_date
  FROM events
  WHERE user_id IS NOT NULL;

  -- 2) ensure the table exists
  CREATE TABLE IF NOT EXISTS users_cumulated (
    user_id      TEXT   NOT NULL,
    date         DATE   NOT NULL,
    dates_active DATE[] NOT NULL,
    PRIMARY KEY(user_id, date)
  );

  -- 3) iterate from first→last day
  d := start_date;
  WHILE d <= end_date LOOP
    WITH
      yesterday AS (
        SELECT *
        FROM users_cumulated
        WHERE date = d - INTERVAL '1 day'
      ),
      today AS (
        SELECT
          user_id::TEXT,
          d AS date_active
        FROM events
        WHERE DATE(event_time) = d
          AND user_id IS NOT NULL
        GROUP BY user_id
      )
    INSERT INTO users_cumulated(user_id, date, dates_active)
    SELECT
      COALESCE(t.user_id, y.user_id)       AS user_id,
      COALESCE(t.date_active, d)           AS date,
      CASE
        WHEN y.dates_active IS NULL        THEN ARRAY[t.date_active]
        WHEN t.date_active IS NULL         THEN y.dates_active
        ELSE ARRAY[t.date_active] || y.dates_active
      END                                  AS dates_active
    FROM today t
    FULL OUTER JOIN yesterday y
      ON t.user_id = y.user_id
    ON CONFLICT (user_id, date) DO UPDATE
      SET dates_active = EXCLUDED.dates_active;

    d := d + INTERVAL '1 day';
  END LOOP;
END
$$ LANGUAGE plpgsql;

-- 1) parameters
WITH params AS (
  SELECT
    DATE '2023-01-31' AS target_day,
    365        AS window_days
),

-- 2) generate the last N days
series AS (
  SELECT
    p.target_day,
    p.window_days,
    (p.target_day - (n * INTERVAL '1 day'))::date AS series_date,
    n
  FROM params p
  CROSS JOIN generate_series(0, (SELECT window_days - 1 FROM params)) AS n
),

-- 3) mark each (user, day) 1/0
flags AS (
  SELECT
    u.user_id,
    s.series_date,
    CASE WHEN u.dates_active @> ARRAY[s.series_date] THEN '1' ELSE '0' END AS flag,
    s.n
  FROM users_cumulated u
  JOIN series s
    ON u.date = (SELECT target_day FROM params)
),

-- 4) aggregate into a TEXT mask, ordering n ASC so series_date DESC (today first)
masks AS (
  SELECT
    user_id,
    string_agg(flag, '' ORDER BY n) AS activity_mask
  FROM flags
  GROUP BY user_id
)

SELECT
  user_id,
  activity_mask,
  -- count ones
  length(activity_mask) - length(replace(activity_mask, '1', '')) AS days_active_count,
  -- today = first character = position 1
  substring(activity_mask FROM 1 FOR 1) = '1' AS is_daily_active,
  -- last 7 days = substring positions 1..7
  position('1' IN substring(activity_mask FROM 1 FOR 7)) > 0 AS is_weekly_active,
  -- any = days_active_count > 0
  (length(activity_mask) - length(replace(activity_mask, '1', ''))) > 0
    AS is_monthly_active
FROM masks
ORDER BY days_active_count DESC;

