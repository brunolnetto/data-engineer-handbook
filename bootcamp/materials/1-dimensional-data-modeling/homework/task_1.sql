drop table if exists actors;
drop type if exists quality_class;

create type quality_class as ENUM ('star', 'good', 'average', 'bad');

create table actors (
    actor_id text,
    actor text,
    year integer,
    films jsonb,
    quality_class quality_class,
    is_active boolean
);
