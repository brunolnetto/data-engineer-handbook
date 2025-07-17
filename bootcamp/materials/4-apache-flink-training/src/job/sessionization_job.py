import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, TableEnvironment, StreamTableEnvironment
from pyflink.table.expressions import lit, col
from pyflink.table.window import Session

# Environment variables
POSTGRES_URL = os.environ.get("POSTGRES_URL")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
KAFKA_URL = os.environ.get("KAFKA_URL")
KAFKA_TOPIC = os.environ.get("KAFKA_TOPIC")
KAFKA_GROUP = os.environ.get("KAFKA_GROUP")
KAFKA_WEB_TRAFFIC_KEY = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
KAFKA_WEB_TRAFFIC_SECRET = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")


def create_sessionized_events_sink_postgres(t_env):
    """
    Creates a PostgreSQL sink table for sessionized events.

    This function creates a table that stores sessionized web traffic data,
    including session metadata and aggregated event information.

    SQL DDL Generated:
    CREATE TABLE sessionized_events (
        session_id VARCHAR,
        ip VARCHAR,
        host VARCHAR,
        session_start TIMESTAMP(3),
        session_end TIMESTAMP(3),
        event_count BIGINT
    ) WITH (
      'connector' = 'jdbc',
      'url' = '{POSTGRES_URL}',
      'table-name' = 'sessionized_events',
      'username' = '{POSTGRES_USER}',
      'password' = '{POSTGRES_PASSWORD}',
      'driver' = 'org.postgresql.Driver'
    );

    Args:
        t_env: Flink TableEnvironment instance

    Returns:
        str: The table name 'sessionized_events'
    """
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            session_id VARCHAR,
            ip VARCHAR,
            host VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
          'connector' = 'jdbc',
          'url' = '{POSTGRES_URL}',
          'table-name' = '{table_name}',
          'username' = '{POSTGRES_USER}',
          'password' = '{POSTGRES_PASSWORD}',
          'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_session_stats_sink_postgres(t_env):
    """
    Creates a PostgreSQL sink table for session statistics.

    This function creates a table that stores aggregated session statistics
    per host, including average events per session and total counts.

    SQL DDL Generated:
    CREATE TABLE session_statistics (
        host VARCHAR,
        avg_events_per_session DOUBLE,
        total_sessions BIGINT,
        total_events BIGINT,
        PRIMARY KEY (host) NOT ENFORCED
    ) WITH (
      'connector' = 'jdbc',
      'url' = '{POSTGRES_URL}',
      'table-name' = 'session_statistics',
      'username' = '{POSTGRES_USER}',
      'password' = '{POSTGRES_PASSWORD}',
      'driver' = 'org.postgresql.Driver'
    );

    Args:
        t_env: Flink TableEnvironment instance

    Returns:
        str: The table name 'session_statistics'
    """
    table_name = 'session_statistics'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            avg_events_per_session DOUBLE,
            total_sessions BIGINT,
            total_events BIGINT,
            PRIMARY KEY (host) NOT ENFORCED
        ) WITH (
          'connector' = 'jdbc',
          'url' = '{POSTGRES_URL}',
          'table-name' = '{table_name}',
          'username' = '{POSTGRES_USER}',
          'password' = '{POSTGRES_PASSWORD}',
          'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_events_source_kafka(t_env):
    """
    Creates a Kafka source table for web traffic events.

    This function creates a table that reads web traffic events from Kafka,
    including URL, referrer, user agent, host, IP, headers, and event timestamp.
    The event_time string is converted to a proper TIMESTAMP using the ISO 8601 pattern.

    SQL DDL Generated:
    CREATE TABLE events (
        url VARCHAR,
        referrer VARCHAR,
        user_agent VARCHAR,
        host VARCHAR,
        ip VARCHAR,
        headers VARCHAR,
        event_time VARCHAR,
        event_timestamp AS TO_TIMESTAMP(event_time, 'yyyy-MM-dd''T''HH:mm:ss.SSS''Z'''),
        WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '30' SECOND
    ) WITH (
      'connector' = 'kafka',
      'properties.bootstrap.servers' = '{KAFKA_URL}',
      'topic' = '{KAFKA_TOPIC}',
      'properties.group.id' = '{KAFKA_GROUP}',
      'properties.security.protocol' = 'SASL_SSL',
      'properties.sasl.mechanism' = 'PLAIN',
      'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_WEB_TRAFFIC_KEY}" password="{KAFKA_WEB_TRAFFIC_SECRET}";',
      'scan.startup.mode' = 'latest-offset',
      'properties.auto.offset.reset' = 'latest',
      'format' = 'json'
    );

    Args:
        t_env: Flink TableEnvironment instance

    Returns:
        str: The table name 'events'
    """
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            url VARCHAR,
            referrer VARCHAR,
            user_agent VARCHAR,
            host VARCHAR,
            ip VARCHAR,
            headers VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '30' SECOND
        ) WITH (
          'connector' = 'kafka',
          'properties.bootstrap.servers' = '{KAFKA_URL}',
          'topic' = '{KAFKA_TOPIC}',
          'properties.group.id' = '{KAFKA_GROUP}',
          'properties.security.protocol' = 'SASL_SSL',
          'properties.sasl.mechanism' = 'PLAIN',
          'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="{KAFKA_WEB_TRAFFIC_KEY}" password="{KAFKA_WEB_TRAFFIC_SECRET}";',
          'scan.startup.mode' = 'latest-offset',
          'properties.auto.offset.reset' = 'latest',
          'format' = 'json'
        );
    """
    t_env.execute_sql(source_ddl)
    return table_name


def sessionization_job():
    """
    Main sessionization job that processes web traffic data.

    This job performs the following operations:
    1. Creates Kafka source table for web traffic events
    2. Creates PostgreSQL sink tables for sessionized data and statistics
    3. Sessionizes data by IP and host with 5-minute gaps
    4. Calculates session statistics per host

    SQL DML Generated:

    Sessionization Query:
    INSERT INTO sessionized_events
    SELECT
        CONCAT(ip, '_', host, '_', CAST(SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS STRING)) as session_id,
        ip,
        host,
        SESSION_START(event_timestamp, INTERVAL '5' MINUTE) as session_start,
        SESSION_END(event_timestamp, INTERVAL '5' MINUTE) as session_end,
        COUNT(*) as event_count
    FROM events
    WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
    GROUP BY
        SESSION(event_timestamp, INTERVAL '5' MINUTE),
        ip,
        host

    Statistics Query:
    INSERT INTO session_statistics
    SELECT
        host,
        AVG(CAST(event_count AS DOUBLE)) as avg_events_per_session,
        COUNT(*) as total_sessions,
        SUM(event_count) as total_events
    FROM sessionized_events
    GROUP BY host

    The job answers the homework questions:
    - Average number of web events per session for Tech Creator users
    - Comparison of results between different hosts
    """
    print('Starting Sessionization Job!')

    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create source and sink tables
        source_table = create_events_source_kafka(t_env)
        sessionized_sink = create_sessionized_events_sink_postgres(t_env)
        stats_sink = create_session_stats_sink_postgres(t_env)

        # Sessionize the data by IP and host with 5-minute gap
        sessionized_query = f"""
            INSERT INTO {sessionized_sink}
            SELECT
                CONCAT(ip, '_', host, '_', CAST(SESSION_START(event_timestamp, INTERVAL '5' MINUTE) AS STRING)) as session_id,
                ip,
                host,
                SESSION_START(event_timestamp, INTERVAL '5' MINUTE) as session_start,
                SESSION_END(event_timestamp, INTERVAL '5' MINUTE) as session_end,
                COUNT(*) as event_count
            FROM {source_table}
            WHERE host IN ('zachwilson.techcreator.io', 'zachwilson.tech', 'lulu.techcreator.io')
            GROUP BY
                SESSION(event_timestamp, INTERVAL '5' MINUTE),
                ip,
                host
        """
        print("Executing sessionization query...")
        t_env.execute_sql(sessionized_query)

        # Calculate session statistics per host
        stats_query = f"""
            INSERT INTO {stats_sink}
            SELECT
                host,
                AVG(CAST(event_count AS DOUBLE)) as avg_events_per_session,
                COUNT(*) as total_sessions,
                SUM(event_count) as total_events
            FROM {sessionized_sink}
            GROUP BY host
        """
        print("Executing statistics query...")
        t_env.execute_sql(stats_query).wait()

        print("Sessionization job completed successfully!")

    except Exception as e:
        print(f"Sessionization job failed: {str(e)}")
        raise e


if __name__ == '__main__':
    sessionization_job()
