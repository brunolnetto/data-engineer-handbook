-- Statement: A monthly, reduced fact table DDL host_activity_reduced
-- 
-- month
-- host
-- hit_array - think COUNT(1)
-- unique_visitors array - think COUNT(DISTINCT user_id)
--
-- Business Logic: Create a monthly aggregated fact table that stores
-- daily metrics as arrays, enabling efficient analysis of host performance
-- over time. This denormalized approach allows quick access to historical
-- daily data without complex joins.
-- 
-- Approach: Arrays store daily values where position 0 = first day of month,
-- position 1 = second day, etc. This enables efficient daily trend analysis
-- and monthly rollups while maintaining daily granularity.
-- 
-- Design: Optimized table structure with constraints for data consistency
-- and indexes for common query patterns.
-- 
-- Note: Array length validation is handled in the application logic rather
-- than database constraints to allow for flexible incremental loading where
-- arrays may be built up day by day.
-- Solution

drop table if exists host_activity_reduced;
create table if not exists host_activity_reduced (
    host text,
    month date,
    hit_array BIGINT[],
    unique_visitors BIGINT[],
    PRIMARY KEY(host, month),
    CONSTRAINT valid_month CHECK (date_trunc('month', month) = month)
);

-- Add indexes for common query patterns
CREATE INDEX idx_host_activity_reduced_month ON host_activity_reduced(month);
CREATE INDEX idx_host_activity_reduced_host ON host_activity_reduced(host);