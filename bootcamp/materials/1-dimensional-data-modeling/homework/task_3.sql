create table actors_history_scd (
    actor_id text,
    actor text,
    start_date integer,
    end_date integer,
    films jsonb,
    quality_class quality_class,
    is_active boolean
);
