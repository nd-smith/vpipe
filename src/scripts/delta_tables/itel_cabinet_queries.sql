-- =============================================================================
-- iTel Cabinet Task Tracking - Sample Queries
-- =============================================================================
-- Example queries for analyzing iTel cabinet task data from the tracking table
-- =============================================================================

-- -----------------------------------------------------------------------------
-- 1. Recent Task Events
-- -----------------------------------------------------------------------------
-- View the 20 most recent task events
SELECT
    event_timestamp,
    event_type,
    task_status,
    assignment_id,
    project_id,
    task_name
FROM itel_cabinet_task_tracking
ORDER BY event_timestamp DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 2. Task Lifecycle View
-- -----------------------------------------------------------------------------
-- See complete lifecycle of a specific task assignment
SELECT
    event_timestamp,
    task_status,
    event_type,
    TIMESTAMPDIFF(MINUTE, LAG(event_timestamp) OVER (ORDER BY event_timestamp), event_timestamp) as minutes_since_last_event
FROM itel_cabinet_task_tracking
WHERE assignment_id = 12345  -- Replace with actual assignment_id
ORDER BY event_timestamp ASC;


-- -----------------------------------------------------------------------------
-- 3. Status Distribution
-- -----------------------------------------------------------------------------
-- Count events by status
SELECT
    task_status,
    COUNT(*) as event_count,
    COUNT(DISTINCT assignment_id) as unique_tasks
FROM itel_cabinet_task_tracking
WHERE year = 2026 AND month = 1  -- Current month
GROUP BY task_status
ORDER BY event_count DESC;


-- -----------------------------------------------------------------------------
-- 4. Task Completion Times
-- -----------------------------------------------------------------------------
-- Analyze time from creation to completion
SELECT
    assignment_id,
    project_id,
    task_created_at,
    task_completed_at,
    DATEDIFF(task_completed_at, task_created_at) as days_to_complete,
    TIMESTAMPDIFF(HOUR, task_created_at, task_completed_at) as hours_to_complete
FROM itel_cabinet_task_tracking
WHERE task_completed_at IS NOT NULL
    AND year = 2026
ORDER BY days_to_complete DESC
LIMIT 50;


-- -----------------------------------------------------------------------------
-- 5. Daily Task Activity
-- -----------------------------------------------------------------------------
-- Tasks created and completed per day
SELECT
    DATE(event_timestamp) as activity_date,
    SUM(CASE WHEN event_type = 'CUSTOM_TASK_ASSIGNED' THEN 1 ELSE 0 END) as tasks_assigned,
    SUM(CASE WHEN event_type = 'CUSTOM_TASK_COMPLETED' THEN 1 ELSE 0 END) as tasks_completed,
    COUNT(DISTINCT assignment_id) as unique_tasks
FROM itel_cabinet_task_tracking
WHERE year = 2026 AND month = 1
GROUP BY DATE(event_timestamp)
ORDER BY activity_date DESC;


-- -----------------------------------------------------------------------------
-- 6. User Assignment Metrics
-- -----------------------------------------------------------------------------
-- Task assignments per user
SELECT
    assigned_to_user_id,
    COUNT(DISTINCT assignment_id) as tasks_assigned,
    COUNT(DISTINCT CASE WHEN task_completed_at IS NOT NULL THEN assignment_id END) as tasks_completed,
    ROUND(COUNT(DISTINCT CASE WHEN task_completed_at IS NOT NULL THEN assignment_id END) * 100.0 / COUNT(DISTINCT assignment_id), 2) as completion_rate_pct
FROM itel_cabinet_task_tracking
WHERE assigned_to_user_id IS NOT NULL
    AND year = 2026
GROUP BY assigned_to_user_id
ORDER BY tasks_assigned DESC;


-- -----------------------------------------------------------------------------
-- 7. Active Tasks
-- -----------------------------------------------------------------------------
-- Tasks currently in progress (not completed)
WITH latest_events AS (
    SELECT
        assignment_id,
        task_status,
        event_timestamp,
        project_id,
        task_name,
        assigned_to_user_id,
        ROW_NUMBER() OVER (PARTITION BY assignment_id ORDER BY event_timestamp DESC) as rn
    FROM itel_cabinet_task_tracking
    WHERE year >= 2026
)
SELECT
    assignment_id,
    task_status,
    event_timestamp as last_updated,
    project_id,
    task_name,
    assigned_to_user_id,
    DATEDIFF(CURRENT_DATE, DATE(event_timestamp)) as days_since_last_update
FROM latest_events
WHERE rn = 1
    AND task_status NOT IN ('completed', 'cancelled')
ORDER BY event_timestamp DESC;


-- -----------------------------------------------------------------------------
-- 8. Project-Level Metrics
-- -----------------------------------------------------------------------------
-- Aggregate task metrics by project
SELECT
    project_id,
    COUNT(DISTINCT assignment_id) as total_tasks,
    COUNT(DISTINCT CASE WHEN task_status = 'completed' THEN assignment_id END) as completed_tasks,
    COUNT(DISTINCT CASE WHEN task_status IN ('assigned', 'in_progress') THEN assignment_id END) as active_tasks,
    AVG(CASE
        WHEN task_completed_at IS NOT NULL AND task_created_at IS NOT NULL
        THEN TIMESTAMPDIFF(HOUR, task_created_at, task_completed_at)
    END) as avg_hours_to_complete
FROM itel_cabinet_task_tracking
WHERE year = 2026
GROUP BY project_id
ORDER BY total_tasks DESC
LIMIT 50;


-- -----------------------------------------------------------------------------
-- 9. Event Processing Lag
-- -----------------------------------------------------------------------------
-- Analyze delay between event occurrence and worker processing
SELECT
    DATE(event_timestamp) as event_date,
    AVG(TIMESTAMPDIFF(SECOND, event_timestamp, processed_timestamp)) as avg_processing_lag_seconds,
    MAX(TIMESTAMPDIFF(SECOND, event_timestamp, processed_timestamp)) as max_processing_lag_seconds,
    COUNT(*) as event_count
FROM itel_cabinet_task_tracking
WHERE year = 2026 AND month = 1
GROUP BY DATE(event_timestamp)
ORDER BY event_date DESC;


-- -----------------------------------------------------------------------------
-- 10. Status Transition Patterns
-- -----------------------------------------------------------------------------
-- Common status transition sequences
WITH transitions AS (
    SELECT
        assignment_id,
        task_status as current_status,
        LAG(task_status) OVER (PARTITION BY assignment_id ORDER BY event_timestamp) as previous_status,
        event_timestamp
    FROM itel_cabinet_task_tracking
    WHERE year = 2026
)
SELECT
    COALESCE(previous_status, 'CREATED') as from_status,
    current_status as to_status,
    COUNT(*) as transition_count
FROM transitions
GROUP BY previous_status, current_status
ORDER BY transition_count DESC
LIMIT 20;


-- -----------------------------------------------------------------------------
-- 11. Time Travel - View Historical Data
-- -----------------------------------------------------------------------------
-- Query table as it existed 7 days ago
SELECT COUNT(*) as total_events_7_days_ago
FROM itel_cabinet_task_tracking VERSION AS OF (SELECT MAX(version) - 7 FROM (DESCRIBE HISTORY itel_cabinet_task_tracking));

-- View table history
DESCRIBE HISTORY itel_cabinet_task_tracking;


-- -----------------------------------------------------------------------------
-- 12. Data Quality Checks
-- -----------------------------------------------------------------------------
-- Identify potential data quality issues
SELECT
    'Missing assignment_id' as issue_type,
    COUNT(*) as count
FROM itel_cabinet_task_tracking
WHERE assignment_id IS NULL
    AND year = 2026

UNION ALL

SELECT
    'Missing project_id' as issue_type,
    COUNT(*) as count
FROM itel_cabinet_task_tracking
WHERE project_id IS NULL
    AND year = 2026

UNION ALL

SELECT
    'Completed without created_at' as issue_type,
    COUNT(*) as count
FROM itel_cabinet_task_tracking
WHERE task_completed_at IS NOT NULL
    AND task_created_at IS NULL
    AND year = 2026;


-- =============================================================================
-- Maintenance Queries
-- =============================================================================

-- Optimize table for better query performance
OPTIMIZE itel_cabinet_task_tracking
ZORDER BY (task_id, assignment_id, task_status);

-- Vacuum old files (after retention period)
-- CAUTION: Cannot time-travel before vacuum retention period
VACUUM itel_cabinet_task_tracking RETAIN 168 HOURS;  -- 7 days

-- Update table statistics
ANALYZE TABLE itel_cabinet_task_tracking COMPUTE STATISTICS;

-- View table properties
DESCRIBE EXTENDED itel_cabinet_task_tracking;

-- View partition information
SHOW PARTITIONS itel_cabinet_task_tracking;
