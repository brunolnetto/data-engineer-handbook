-- Statement: A DDL for an `user_devices_cumulated` table that has:
--   - a `device_activity_datelist` which tracks a users active days by `browser_type`
--   - data type here should look similar to `MAP<STRING, ARRAY[DATE]>`
--     - or you could have `browser_type` as a column with multiple rows for each user (either way works, just be consistent!)
-- 
-- Business Logic: Track user activity by browser type over time, enabling
-- analysis of user engagement patterns across different browsers.
-- 
-- Approach: Using browser_type as separate rows (normalized approach)
-- instead of MAP<STRING, ARRAY[DATE]> for better query performance and
-- easier maintenance. This allows efficient filtering and aggregation.
-- 
-- Design: Simple structure with composite primary key and validation
-- constraints to ensure data integrity.
-- Solution:

drop table if exists user_devices_cumulated;
create table if not exists user_devices_cumulated (
    user_id numeric,
    date date,
    browser_type text,
    device_activity_datelist DATE[],
    primary key(user_id, date, browser_type),
    constraint valid_browser_type check (browser_type != ''),
    constraint valid_date check (date is not null)
);