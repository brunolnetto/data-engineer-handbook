-- =============================================
-- Populate players table for all seasons
-- =============================================

drop type if exists season_stats cascade;
drop type if exists scoring_class cascade;
drop type if exists career_status cascade;
create type season_stats as (
    season INTEGER,
    gp INTEGER,
    pts REAL,
    reb REAL,
    ast REAL
);
create type scoring_class as enum (
    'star', 'good', 'average', 'bad'
);
create type career_status as enum (
    'New', 'Retired', 'Continued Playing', 'Returned from Retirement', 'Stayed Retired'
);
drop table players;
CREATE TABLE players (
    player_name text,
    height text,
    college text,
    country text,
    draft_year text,
    draft_round text,
    draft_number text,
    season_stats season_stats[],
    scoring_class scoring_class,
    career_status career_status,
    years_since_last_season integer,
    current_season integer,
    PRIMARY KEY(player_name, current_season)
);


-- Assumes types and tables are already created as in task_1.sql

-- 1. Function: Upsert players for a single season
drop function if exists upsert_players_for_season(integer) cascade;
create or replace function upsert_players_for_season(target_season integer)
returns void as $$
begin
    with yesterday as (
        select * from players where current_season = target_season - 1
    ),
    today as (
        select * from player_seasons where season = target_season
    )
    insert into players (
        player_name, height, college, country, draft_year, draft_round, draft_number,
        season_stats, scoring_class, career_status, years_since_last_season, current_season
    )
    select
        coalesce(t.player_name, y.player_name) as player_name,
        coalesce(t.height, y.height) as height,
        coalesce(t.college, y.college) as college,
        coalesce(t.country, y.country) as country,
        coalesce(t.draft_year, y.draft_year) as draft_year,
        coalesce(t.draft_round, y.draft_round) as draft_round,
        coalesce(t.draft_number, y.draft_number) as draft_number,
        coalesce(y.season_stats, '{}') || (
            case when t.season is not null then
                array[row(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats]
            else '{}'::season_stats[] end
        ) as season_stats,
        case
            when t.season is not null then (
                case
                    when t.pts > 20 then 'star'::scoring_class
                    when t.pts > 15 then 'good'::scoring_class
                    when t.pts > 10 then 'average'::scoring_class
                    else 'bad'::scoring_class
                end
            ) else y.scoring_class
        end as scoring_class,
        CASE
            WHEN t.season IS NOT NULL THEN
                CASE
                WHEN y.career_status IS NULL THEN 'New'::career_status
                WHEN y.years_since_last_season = 0 AND (y.career_status = 'New' OR y.career_status = 'Continued Playing') THEN 'Continued Playing'::career_status
                WHEN y.years_since_last_season > 0 AND (y.career_status = 'Retired' OR y.career_status = 'Stayed Retired') THEN 'Returned from Retirement'::career_status
                ELSE y.career_status
                END
            WHEN t.season IS NULL THEN
                CASE
                WHEN y.years_since_last_season = 0 THEN 'Retired'::career_status
                WHEN y.years_since_last_season > 0 THEN 'Stayed Retired'::career_status
                ELSE y.career_status
                END
            END AS career_status,
        case
            when t.season is not null then 0
            else y.years_since_last_season + 1
        end as years_since_last_season,
        coalesce(t.season, y.current_season + 1) as current_season
    from today t
    full outer join yesterday y on t.player_name = y.player_name;
end;
$$ language plpgsql;

-- 2. Procedure: Loop over all seasons
drop procedure if exists populate_players_all_seasons(integer, integer) cascade;
create or replace procedure populate_players_all_seasons(min_season integer, max_season integer)
language plpgsql as $$
declare
    yr integer;
begin
    for yr in min_season..max_season loop
        perform upsert_players_for_season(yr);
    end loop;
end;
$$;

-- 3. Usage Example
call populate_players_all_seasons(1996, 2022);
