# Homework 1: Dimensional Data Modeling

## Overview
This homework focuses on dimensional data modeling using SQL. You will design and implement a star schema, create tables, and write queries to support analytical workloads. The tasks are based on a movie actors dataset and cover both DDL and DML operations.

## Structure
- `task_1.sql`: Initial table creation and data loading
- `task_2.sql`: Cumulative actor table population function
- `task_3.sql`: DDL for `actors_history_scd` (Slowly Changing Dimension Type 2)
- `task_4.sql`: Backfill query for `actors_history_scd` (full-refresh SCD logic)
- `task_5.sql`: Incremental SCD Type 2 merge query for `actors_history_scd`
- `homework.md`: Assignment description and requirements
- `.gitkeep`: Placeholder for version control
- `homework_1.zip`: Archive of all homework 1 files

## Learning Objectives
- Practice dimensional modeling concepts (star schema, SCD)
- Write DDL and DML SQL for analytical data models
- Implement SCD Type 2 logic for historical tracking
- Use window functions, CTEs, and advanced SQL features

## How to Use
1. Review `homework.md` for assignment instructions.
2. Examine each `task_X.sql` file for the corresponding step in the workflow.
3. Run the SQL scripts in order on a PostgreSQL-compatible database.
4. Use the provided DDL and queries as reference for data modeling best practices.

## Notes
- The SQL files are self-contained and can be run independently if the required tables exist.
- The SCD logic in `task_4.sql` and `task_5.sql` is suitable for both backfill and incremental ETL scenarios. 