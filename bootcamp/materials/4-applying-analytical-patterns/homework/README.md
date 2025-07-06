# Week 4: Applying Analytical Patterns - Homework

This folder contains SQL solutions for the Week 4 homework, which focuses on state change tracking, advanced aggregation, and window function analytics using NBA data.

## File Overview & Learning Objectives

### 1. State Change Tracking for Players
- **File:** `task_1.sql`
- **Purpose:** Demonstrates how to track player career states (New, Retired, Continued Playing, Returned from Retirement, Stayed Retired) across all seasons using SCD logic and procedural SQL. Shows how to automate historical state tracking in a sports analytics context.
- **Skills Practiced:** PL/pgSQL functions and procedures, SCD logic, handling edge cases in time series data.

### 2. GROUPING SETS Aggregations on Game Details
- **File:** `task_2.sql`
- **Purpose:** Shows how to use `GROUPING SETS` for efficient multi-dimensional aggregation. Answers questions about player and team performance across different dimensions (team, player, season).
- **Skills Practiced:** Advanced SQL aggregation, multi-level reporting, using `GROUPING SETS` for flexible analytics.

### 3. Window Functions on Game Details
- **File:** `task_3_1.sql`
- **Purpose:** Uses window functions to analyze rolling performance, specifically the most games a team has won in any 90-game stretch.
- **Skills Practiced:** Window functions, moving window analytics, sports performance analysis.

- **File:** `task_3_2.sql`
- **Purpose:** Uses window functions to find the longest streak of games where LeBron James scored over 10 points, illustrating streak and run-length analysis in SQL.
- **Skills Practiced:** Windowed running sums, streak detection, player performance analytics.

## How to Use
- Review each SQL file for the corresponding analytical pattern or question.
- Run the queries in a PostgreSQL-compatible environment with the required tables (`players`, `player_seasons`, `game_details`, `games`, etc.).
- Adjust table or column names if your schema differs.

## Learning Outcomes
- Understand how to track and analyze state changes in time series data.
- Apply advanced SQL features like `GROUPING SETS` and window functions to real-world sports analytics problems.
- Gain experience with multi-dimensional aggregation and streak analysis in SQL.

---

**Questions answered:**
- State change tracking for NBA players
- Multi-dimensional aggregation of game stats
- Advanced window analytics for team and player performance 