# Week 4: Applying Analytical Patterns - Homework

This folder contains SQL solutions for the Week 4 homework, focused on state change tracking, advanced aggregation, and window function analytics using NBA data.

## Homework Questions & File Mapping

### 1. State Change Tracking for Players
- **File:** `task_1.sql`
- **Purpose:** Tracks player career states (New, Retired, Continued Playing, Returned from Retirement, Stayed Retired) across all seasons using SCD logic and procedural SQL.

### 2. GROUPING SETS Aggregations on Game Details
- **File:** `task_2.sql`
- **Purpose:** Uses `GROUPING SETS` to efficiently aggregate `game_details` by (team, player), (season, player), and (team). Supports multi-dimensional analysis for points and wins.

### 3. Who scored the most points for one team?
- **File:** `task_3.sql`
- **Purpose:** Identifies the player-team combination with the highest total points.

### 4. Who scored the most points in one season?
- **File:** `task_4.sql`
- **Purpose:** Identifies the player-season combination with the highest total points.

### 5. Which team has won the most games?
- **File:** `task_5.sql`
- **Purpose:** Identifies the team with the most total wins.

### 6. What is the most games a team has won in a 90 game stretch?
- **File:** `task_6.sql`
- **Purpose:** Uses window functions to find the maximum number of wins by a team in any rolling 90-game window.

### 7. How many games in a row did LeBron James score over 10 points?
- **File:** `task_7.sql`
- **Purpose:** Uses window functions to find the longest streak of games where LeBron James scored over 10 points.

## Learning Outcomes
- Track and analyze state changes in time series data.
- Apply `GROUPING SETS` and window functions to real-world sports analytics problems.
- Perform multi-dimensional aggregation and streak analysis in SQL.

## How to Use
- Review each SQL file for the corresponding analytical pattern or question.
- Run the queries in a PostgreSQL-compatible environment with the required tables (`players`, `player_seasons`, `game_details`, `games`, etc.).
- Adjust table or column names if your schema differs. 