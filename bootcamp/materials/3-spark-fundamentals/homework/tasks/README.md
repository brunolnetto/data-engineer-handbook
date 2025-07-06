# Spark Fundamentals Homework - PostgreSQL to SparkSQL Conversion

## Overview

This homework converts two PostgreSQL queries from Week 1 (Dimensional Data Modeling) to SparkSQL:

1. **Task 4**: Full-refresh backfill query for `actors_history_scd` 
2. **Task 5**: Incremental SCD Type 2 merge query for `actors_history_scd`

## Requirements

### Original PostgreSQL Queries

The original queries can be found in:
- `bootcamp/materials/1-dimensional-data-modeling/homework/task_4.sql` - Backfill query
- `bootcamp/materials/1-dimensional-data-modeling/homework/task_5.sql` - Incremental query

### PySpark Implementation

- **`src/jobs/actors_scd_backfill_job.py`**: Backfill transformation converted to SparkSQL
- **`src/jobs/actors_scd_incremental_job.py`**: Incremental transformation converted to SparkSQL
- **`src/jobs/actors_scd_job.py`**: Combined implementation with both transformations
- **`src/tests/test_actors_scd.py`**: Comprehensive test suite for both transformations

## Implementation Details

### 1. Backfill Transformation (`actors_scd_backfill_job.py`)

**Purpose**: Populate the entire `actors_history_scd` table from scratch using SCD Type 2 logic.

**Key Features**:
- Identifies change points when actor quality class or activity status changes
- Creates streaks for periods of consistent state
- Uses `struct()` for state vector comparison (equivalent to PostgreSQL `ROW`)
- Uses `MAX(CAST(is_active AS INT)) = 1` for boolean aggregation (equivalent to PostgreSQL `BOOL_OR`)
- Handles complex array types for film information

**PostgreSQL to SparkSQL Conversions**:
- `ROW(quality_class, is_active)::TEXT` → `struct(quality_class, is_active)`
- `IS DISTINCT FROM` → `!=`
- `BOOL_OR(is_active)` → `MAX(CAST(is_active AS INT)) = 1`
- `LEFT JOIN LATERAL` → `LEFT JOIN` with subquery

### 2. Incremental Transformation (`actors_scd_incremental_job.py`)

**Purpose**: Incrementally update `actors_history_scd` with new data while maintaining SCD Type 2 history.

**Key Features**:
- Handles unchanged actors (extend open ranges)
- Handles changed actors (close old records, open new ones)
- Handles new actors (create new records)
- Maintains referential integrity with proper valid_from/valid_to dates

**PostgreSQL to SparkSQL Conversions**:
- `UNNEST(ARRAY[...])` → Separate `UNION ALL` queries
- `USING(actor_id)` → `ON cs.actor_id = lo.actor_id`
- `IS DISTINCT FROM` → `!=`

## Test Coverage

### Backfill Tests
- `test_actors_scd_backfill_single_streak`: Actor with no changes
- `test_actors_scd_backfill_multiple_streaks`: Actor with quality class changes
- `test_actors_scd_backfill_activity_changes`: Actor with activity status changes
- `test_actors_scd_backfill_multiple_actors`: Multiple actors with different scenarios
- `test_actors_scd_empty_input`: Edge case with empty data

### Incremental Tests
- `test_actors_scd_incremental_unchanged_actor`: Actor with no changes
- `test_actors_scd_incremental_changed_actor`: Actor with quality class changes
- `test_actors_scd_incremental_new_actor`: New actor not in history
- `test_actors_scd_incremental_mixed_scenarios`: Multiple scenarios in one test

## Data Schema

### Actors Table
```sql
actor_id STRING,
actor STRING,
year INT,
films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>,
quality_class STRING,
is_active BOOLEAN
```

### Actors History SCD Table
```sql
actor_id STRING,
actor STRING,
valid_from INT,
valid_to INT,
films ARRAY<STRUCT<film:STRING,votes:INT,rating:DOUBLE,filmid:STRING>>,
quality_class STRING,
is_active BOOLEAN
```

## Running the Tests

### Prerequisites
- PySpark installed
- chispa library for DataFrame testing

### Run Tests
```bash
cd bootcamp/materials/3-spark-fundamentals/homework/tasks
python3 -m pytest src/tests/test_actors_scd.py -v
```

### Run Individual Test
```bash
python3 -m pytest src/tests/test_actors_scd.py::test_actors_scd_backfill_single_streak -v
```

### Run Individual Jobs
```bash
# Run backfill job
python3 src/jobs/actors_scd_backfill_job.py

# Run incremental job
python3 src/jobs/actors_scd_incremental_job.py
```

## Business Logic

### SCD Type 2 Implementation
- **Valid From**: Start year of the current state
- **Valid To**: End year of the current state (NULL for open records)
- **Change Detection**: Monitors quality_class and is_active changes
- **Streak Identification**: Groups consecutive years with the same state
- **Film Information**: Uses films from the last year of each streak

### Quality Class Logic
- **Star**: Average rating > 8
- **Good**: Average rating > 7
- **Average**: Average rating > 6
- **Bad**: Average rating ≤ 6

### Activity Status
- **Active**: Actor has films in the current year
- **Inactive**: Actor has no films in the current year

## File Structure

```
src/
├── jobs/
│   ├── actors_scd_backfill_job.py      # Backfill query conversion
│   ├── actors_scd_incremental_job.py   # Incremental query conversion
│   └── actors_scd_job.py               # Combined implementation
├── tests/
│   ├── test_actors_scd.py              # Comprehensive test suite
│   └── conftest.py                     # Spark session configuration
└── __init__.py
```

## Query Conversion Summary

### Backfill Query (Task 4)
- **Input**: `actors` table with yearly snapshots
- **Output**: `actors_history_scd` table with SCD Type 2 history
- **Logic**: Identifies state changes and creates historical records
- **SparkSQL Features**: Window functions, CTEs, struct comparisons

### Incremental Query (Task 5)
- **Input**: Current `actors` snapshot + existing `actors_history_scd`
- **Output**: Updated `actors_history_scd` with new records
- **Logic**: Handles unchanged, changed, and new actors
- **SparkSQL Features**: UNION ALL, JOINs, conditional logic

Both query conversions maintain the exact business logic of the original PostgreSQL queries while leveraging SparkSQL's distributed processing capabilities. 