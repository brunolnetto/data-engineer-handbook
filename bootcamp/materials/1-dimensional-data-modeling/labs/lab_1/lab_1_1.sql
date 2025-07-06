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
    years_since_last_season integer,
    current_season integer,
    PRIMARY KEY(player_name, current_season)
);

WITH yesterday AS (
    SELECT
        *
    FROM players
    WHERE current_season = 2000
),
today AS (
    SELECT
        *
    FROM player_seasons
    WHERE season = 2001
)

insert into players (
SELECT
    COALESCE(t.player_name, y.player_name) AS player_name,
    COALESCE(t.height, y.height) AS height,
    COALESCE(t.college, y.college) AS college,
    COALESCE(t.country, y.country) AS country,
    COALESCE(t.draft_year, y.draft_year) AS draft_year,
    COALESCE(t.draft_round, y.draft_round) AS draft_round,
    COALESCE(t.draft_number, y.draft_number) AS draft_number,
    COALESCE(y.season_stats, '{}') || (
        case when t.season is not null then
            ARRAY[
                ROW(t.season, t.gp, t.pts, t.reb, t.ast)::season_stats
            ]
        else '{}'::season_stats[]
        end
    ) AS season_stats,
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
    case
        when t.season is not null then 0
    else y.years_since_last_season+1
    end as years_since_last_season,
    coalesce(t.season, y.current_season+1) as current_season

FROM today t
FULL OUTER JOIN yesterday y
ON t.player_name = y.player_name
);

select
    player_name,
    (season_stats[cardinality(season_stats)]::season_stats).pts/(
        case when (season_stats[1]::season_stats).pts = 0 then 1 else (season_stats[1]::season_stats).pts end
    ) ratio
from players
where
    current_season = 2001 and
    scoring_class = 'star'
order by ratio desc;