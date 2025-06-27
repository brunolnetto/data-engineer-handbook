-- drop table if exists users_cumulated;
create table if not exists users_cumulated (
    user_id TEXT,
    date DATE, -- The current date for the user
    dates_active DATE[], -- The list of dates in the past where the user was active
    PRIMARY KEY(USER_ID, date)
);

insert into users_cumulated
with yesterday as (
    select
        *
    from
        users_cumulated
    where
        date = DATE('2023-01-30')
),
today as (
    select
        user_id,
        date(cast(event_time as timestamp)) as date_active
    from
        events
    where
        DATE(CAST(event_time as TIMESTAMP)) = DATE('2023-01-31') and
        user_id is not NULL
    group by user_id, date(cast(event_time as timestamp))
)

select
    coalesce(t.user_id::text, y.user_id) as user_id,
    coalesce(t.date_active, y.date + INTERVAL '1 day') as date,
    (
        case
            when y.dates_active is null then array[t.date_active]
            when t.date_active is null then y.dates_active
            else array[t.date_active] || y.dates_active
        end
    ) as dates_active
from
    today t
full outer join
    yesterday y
on t.user_id::TEXT = y.user_id::TEXT;

-- Parameters (you can wrap into a function later)
-- 1) Define your parameters
WITH params AS (
  SELECT
    DATE '2023-01-31' AS target_day,
    31       AS window_days
),

-- 2) Generate the date series
series AS (
  SELECT
    p.target_day,
    p.window_days,
    ((p.target_day - (p.window_days - 1) * INTERVAL '1 day')::DATE
      + (n * INTERVAL '1 day'))::date     AS series_date
  FROM params p
  CROSS JOIN generate_series(0, (SELECT window_days - 1 FROM params)) AS n
),

-- 3) Pull in your snapshot of active dates
users AS (
  SELECT *
  FROM users_cumulated
  WHERE date = (SELECT target_day FROM params)
),

-- 4) For each user × day, compute the bit-value placeholder
placeholder_ints AS (
  SELECT
    u.user_id,
    s.window_days,
    CASE
      WHEN u.dates_active @> ARRAY[s.series_date] THEN
        -- position = window_days-1 - days_diff
        CAST(
          POW(
            2,
            ( s.window_days - 1
            - (s.target_day - s.series_date)
            )
          )
          AS BIGINT
        )
      ELSE 0
    END AS placeholder_int
  FROM users u
  CROSS JOIN series s
),

-- 5) Sum up into a full 64-bit word, carrying window_days forward
users_activity_bitwise AS (
  SELECT
    user_id,
    window_days,
    -- use a 64-bit container
    SUM(placeholder_int)::bigint AS full_bitmap_int
  FROM placeholder_ints
  GROUP BY user_id, window_days
),

-- 6) Final projection: trim to window_days bits, produce flags
users_activity AS (
  SELECT
    user_id,

    -- take the rightmost window_days bits
    RIGHT(full_bitmap_int::bit(64)::text, window_days) AS activity_mask,

    -- count set bits in those N bits
    bit_count(RIGHT(full_bitmap_int::bit(64)::text, window_days)::bit(64)) AS activity_count,

    -- daily active? LSB = position 0
    ( full_bitmap_int & (1::bigint << (window_days - 1)) ) <> 0 AS is_daily_active,

    -- weekly active? check any of the rightmost 7 bits
    ( full_bitmap_int
        & ( ((2^7 - 1)::bigint) << (window_days - 7) )
    ) <> 0 AS is_weekly_active,

    -- monthly active? any of the rightmost window_days bits
    full_bitmap_int <> 0 AS is_monthly_active

  FROM users_activity_bitwise
)

SELECT *
FROM users_activity
ORDER BY activity_count DESC, activity_mask desc;
