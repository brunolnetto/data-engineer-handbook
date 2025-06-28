drop table if exists array_metrics;
create table array_metrics (
    user_id NUMERIC,
    month_start DATE,
    metric_name TEXT,
    metric_array REAL[],
    PRIMARY KEY(user_id, month_start, metric_name)
);

CREATE OR REPLACE PROCEDURE daily_metric_rollforward(_day DATE)
LANGUAGE plpgsql
AS $$
DECLARE
  _month_start DATE := date_trunc('month', _day)::DATE;
BEGIN
  WITH
  -- 1) yesterday’s snapshot for this month (may be empty on day=1)
  yesterday AS (
    SELECT user_id, metric_array
    FROM array_metrics
    WHERE month_start = _month_start
      AND metric_name = 'site_hits'
  ),

  -- 2) today’s raw counts
  today AS (
    SELECT
      user_id,
      COUNT(*) AS hits
    FROM events
    WHERE user_id IS NOT NULL
      AND DATE(event_time) = _day
    GROUP BY user_id
  ),

  -- 3) merge users who had history or had hits today
  merged AS (
    SELECT
      COALESCE(t.user_id, y.user_id) AS user_id,
      y.metric_array,
      COALESCE(t.hits, 0)             AS today_hits
    FROM today t
    FULL OUTER JOIN yesterday y USING (user_id)
  )

  -- 4) upsert: append today_hits (or initialize array on first day)
  INSERT INTO array_metrics(user_id, month_start, metric_name, metric_array)
  SELECT
    m.user_id,
    _month_start,
    'site_hits' AS metric_name,
    CASE
      -- we have yesterday’s array: just append
      WHEN m.metric_array IS NOT NULL THEN
        m.metric_array || ARRAY[m.today_hits]
      -- no history yet: pad zeros up to day‑1 then append
      ELSE
        CASE
          WHEN (_day - _month_start) > 0 THEN
            array_fill(0::REAL, ARRAY[_day - _month_start])
          ELSE
            '{}'::REAL[]
        END
        || ARRAY[m.today_hits]
    END
  FROM merged m
  ON CONFLICT (user_id, month_start, metric_name)
  DO UPDATE
    SET metric_array = EXCLUDED.metric_array;
END
$$;

-- Usage: just call it for each day you want to roll forward
CREATE OR REPLACE PROCEDURE rollforward_range(_start DATE, _end DATE)
LANGUAGE plpgsql
AS $$
DECLARE
    _current DATE := _start;
BEGIN
    WHILE _current <= _end LOOP
        CALL daily_metric_rollforward(_current);
        RAISE NOTICE 'Processing date: %', _current;
        _current := _current + INTERVAL '1 day';
    END LOOP;
END;
$$;

--
DO $$
DECLARE
    _start DATE;
    _end   DATE;
BEGIN
    SELECT MIN(DATE(event_time)), MAX(DATE(event_time))
    INTO _start, _end
    FROM events;

    IF _start IS NOT NULL AND _end IS NOT NULL THEN
        CALL rollforward_range(_start, _end);
    ELSE
        RAISE NOTICE 'No events found in events table. Skipping.';
    END IF;
END;
$$;


WITH exploded AS (
  SELECT
    metric_name,
    month_start,
    index,
    SUM(value) AS daily_sum
  FROM (
    SELECT
      metric_name,
      month_start,
      val AS value,
      idx AS index
    FROM array_metrics,
    LATERAL unnest(metric_array) WITH ORDINALITY AS u(val, idx)
  ) t
  GROUP BY metric_name, month_start, index
)

SELECT
  metric_name,
  month_start + (index - 1) * INTERVAL '1 day' AS day,
  daily_sum AS value
FROM exploded
ORDER BY metric_name, day;
