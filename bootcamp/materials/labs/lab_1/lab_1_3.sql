drop table if exists edges;
drop table if exists vertices;
drop type if exists vertex_type cascade;
drop type if exists edge_type cascade;

create type vertex_type as ENUM('player', 'team', 'game');

create table vertices (
    identifier text,
    type vertex_type,
    properties JSON,
    PRIMARY KEY (identifier, type)
);


create type edge_type as ENUM(
    'plays_against',
    'shares_team',
    'plays_in',
    'plays_on'
);

create table edges (
    subject_identifier text,
    subject_type vertex_type,
    object_identiifier  text,
    object_type vertex_type,
    edge_type edge_type,
    properties JSON,
    primary key (subject_identifier, subject_type, object_identiifier, object_type, edge_type)
);

-- Games vertices
insert into vertices (
select
    game_id as identifier,
    'game'::vertex_type as vertex_type,
    json_build_object(
        'pts_home', pts_home,
        'pts_away', pts_away,
        'winning_team', CASE when home_team_wins = 1 then home_team_id else visitor_team_id end
    ) as properties
from games);

-- Players vertices
insert into vertices
with players_agg as (
    select
        player_id as identifier,
        max(player_name) as player_name,
        count(1) as number_of_games,
        sum(pts) as total_points,
        array_agg(distinct team_id) as teams
    from game_details
    group by player_id
)

select
    identifier,
    'player'::vertex_type as vertex_type,
    json_build_object(
        'player_name', player_name,
        'number_of_games', number_of_games,
        'total_points', total_points,
        'teams', teams
    ) as properties
from players_agg;

-- Teams vertices
insert into vertices
with teams_deduped as (
    select
        *,
        row_number() over (partition by team_id) as  row_num
    from
        teams
)
select
    team_id as identifier,
    'team'::vertex_type as vertex_type,
    json_build_object(
        'abbreviation', abbreviation,
        'nickname', nickname,
        'city', city,
        'arena', arena,
        'year_founded', yearfounded
    ) as properties
from teams_deduped
where row_num = 1;

select type, count(1) from vertices group by 1;

-- Player-player edge (undirected)
insert into edges
with deduped as (
    select
        *,
        row_number() over (partition by player_id, game_id) as row_num
    from game_details
),
filtered as (
    select * from deduped where row_num = 1
),
pairs AS (
  SELECT
    -- pick a canonical ordering for the player‑pair
    LEAST(f1.player_id, f2.player_id)   AS p1_id,
    GREATEST(f1.player_id, f2.player_id) AS p2_id,
    -- but each one’s actual team in that game:
    CASE WHEN f1.player_id < f2.player_id THEN f1.team_abbreviation
         ELSE f2.team_abbreviation
    END AS team_p1,
    CASE WHEN f1.player_id < f2.player_id THEN f2.team_abbreviation
         ELSE f1.team_abbreviation
    END AS team_p2,
    -- points likewise go to the right slot
    CASE WHEN f1.player_id < f2.player_id THEN f1.pts
         ELSE f2.pts
    END AS pts_p1,
    CASE WHEN f1.player_id < f2.player_id THEN f2.pts
         ELSE f1.pts
    END AS pts_p2,
    -- edge type is the same logical test
    CASE
      WHEN f1.team_abbreviation = f2.team_abbreviation THEN 'shares_team'::edge_type
      ELSE 'plays_against'::edge_type
    END AS edge_type
  FROM filtered f1
  JOIN filtered f2
    ON f1.game_id = f2.game_id
   AND f1.player_id <> f2.player_id
),
aggregated as (
    SELECT
      p1_id, p2_id, edge_type,
      COUNT(*)        AS num_games,
      SUM(pts_p1)     AS total_pts_p1,
      SUM(pts_p2)     AS total_pts_p2
    FROM pairs
    GROUP BY 1,2,3
)
select
    p1_id as subject_identifier,
    'player'::vertex_type as subject_type,
    p2_id as object_identifier,
    'player'::vertex_type as subject_type,
    edge_type,
    json_build_object(
        'num_games', num_games,
        'subject_points', total_pts_p1,
        'object_points', total_pts_p2
    )
    as properties
from
    aggregated;

-- Player-game edges
insert into edges
with deduped as (
    select
        *,
        row_number() over (partition by player_id, game_id) as row_num
    from game_details
),
filtered as (
    select *
    from deduped
    where row_num = 1
)

select
    player_id as subject_identifier,
    'player'::vertex_type as subject_type,
    game_id as object_identifier,
    'game'::vertex_type as object_type,
    'plays_in'::edge_type as edge_type,
    json_build_object(
        'start_position', start_position,
        'pts', pts,
        'team_id', team_id,
        'team_abbreviation', team_abbreviation
    ) as properties
from filtered;

-- Player-team edges
insert into edges
with deduped as (
    select
        *,
        row_number() over (partition by player_id, game_id) as row_num
    from game_details
),
filtered as (
    select *
    from deduped
    where row_num = 1
),
pairs as (
    select
        player_id as subject_identifier,
        team_id object_identifier,
        sum(pts) as player_pts,
        count(distinct game_id) as games_count
    from filtered
    group by 1, 2
)

select
    subject_identifier,
    'player'::vertex_type as subject_type,
    object_identifier,
    'team'::vertex_type as subject_type,
    'plays_on'::edge_type as edge_type,
    json_build_object(
        'pts', player_pts,
        'games', games_count
    ) as properties
from pairs;

select type, count(1) from vertices group by 1;
select edge_type, count(1) from edges group by 1;

select
    v.properties->>'player_name' as player_subject,
    e.object_identiifier as player_object,
    cast(v.properties->>'total_points' as REAL)/(
        case
            when cast(v.properties->>'number_of_games' as REAL) = 0 then 1
            else cast(v.properties->>'number_of_games' as REAL)
        end
    ) as points_per_game,
    e.properties->>'subject_points' as total_points,
    e.properties->>'num_games' as total_games
from vertices v join edges e
on
    v.identifier = e.subject_identifier and
    v.type = e.subject_type
where e.object_type = 'player'::vertex_type;
