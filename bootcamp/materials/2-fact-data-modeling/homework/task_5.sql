-- Statement: A DDL for hosts_cumulated table
--
-- a host_activity_datelist which logs to see which dates each host is experiencing any activity
-- 
-- Business Logic: Track website host activity over time to understand
-- traffic patterns and identify which hosts are experiencing engagement
-- on specific dates. This enables analysis of host popularity and
-- traffic distribution across different time periods.
-- 
-- Approach: Simple structure with host as the entity, date for temporal
-- tracking, and an array of activity dates for efficient querying.
-- The array approach allows quick lookups of activity patterns.
-- 
-- Design: Clean table structure with appropriate constraints to ensure
-- data integrity and performance.
-- Solution:

drop table if exists hosts_cumulated;
create table hosts_cumulated (
    host text,
    date DATE,
    host_activity_datelist DATE[],
    PRIMARY KEY(host, date),
    CONSTRAINT valid_host CHECK (host != ''),
    CONSTRAINT valid_date CHECK (date IS NOT NULL)
);