# Create a Flink job that sessionizes the input data by IP address and host with a 5-minute session window.

import os
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import EnvironmentSettings, DataTypes, StreamTableEnvironment
from pyflink.table.expressions import col, lit
from pyflink.table.window import Session

def create_sessionized_events_sink_postgres(t_env):
    table_name = 'sessionized_events'
    sink_ddl = f"""
        CREATE TABLE {table_name} (
            host VARCHAR,
            ip VARCHAR,
            session_start TIMESTAMP(3),
            session_end TIMESTAMP(3),
            event_count BIGINT
        ) WITH (
            'connector' = 'jdbc',
            'url' = '{os.environ.get("POSTGRES_URL")}',
            'table-name' = '{table_name}',
            'username' = '{os.environ.get("POSTGRES_USER", "postgres")}',
            'password' = '{os.environ.get("POSTGRES_PASSWORD", "postgres")}',
            'driver' = 'org.postgresql.Driver'
        );
    """
    t_env.execute_sql(sink_ddl)
    return table_name


def create_sessionized_events_source_kafka(t_env):
    kafka_key = os.environ.get("KAFKA_WEB_TRAFFIC_KEY", "")
    kafka_secret = os.environ.get("KAFKA_WEB_TRAFFIC_SECRET", "")
    table_name = "events"
    pattern = "yyyy-MM-dd''T''HH:mm:ss.SSS''Z''"
    source_ddl = f"""
        CREATE TABLE {table_name} (
            ip VARCHAR,
            host VARCHAR,
            url VARCHAR,
            event_time VARCHAR,
            event_timestamp AS TO_TIMESTAMP(event_time, '{pattern}'),
            WATERMARK FOR event_timestamp AS event_timestamp - INTERVAL '10' SECOND  
        ) WITH (
            'connector' = 'kafka',
            'properties.bootstrap.servers' = '{os.environ.get('KAFKA_URL')}',
            'topic' = '{os.environ.get('KAFKA_TOPIC')}',
            'properties.group.id' = '{os.environ.get('KAFKA_GROUP')}',
            'properties.security.protocol' = 'SASL_SSL',
            'properties.sasl.mechanism' = 'PLAIN',
            'properties.sasl.jaas.config' = 'org.apache.flink.kafka.shaded.org.apache.kafka.common.security.plain.PlainLoginModule required username=\"{kafka_key}\" password=\"{kafka_secret}\";',
            'scan.startup.mode' = 'latest-offset',
            'properties.auto.offset.reset' = 'latest',
            'format' = 'json'
        );
        """
    print(source_ddl)
    t_env.execute_sql(source_ddl)
    return table_name

def sessionize_events():
    # Set up the execution environment
    env = StreamExecutionEnvironment.get_execution_environment()
    env.enable_checkpointing(10000)
    env.set_parallelism(3)

    # Set up the table environment
    settings = EnvironmentSettings.new_instance().in_streaming_mode().build()
    t_env = StreamTableEnvironment.create(env, environment_settings=settings)

    try:
        # Create Kafka table
        source_table = create_sessionized_events_source_kafka(t_env)
        
        # Create Postgres sink table
        sessionized_sink_table = create_sessionized_events_sink_postgres(t_env)

        # Define session window logic
        t_env.from_path(source_table)\
            .window(
                Session.with_gap(lit(5).minutes)
                .on(col("event_timestamp"))
                .alias("s")
            )\
            .group_by(
                col("ip"),
                col("host"),
                col("s")
            )\
            .select(
                col("host"),
                col("ip"),
                col("s").start.alias("session_start"),
                col("s").end.alias("session_end"),
                col("host").count.alias("event_count")
            )\
            .execute_insert(sessionized_sink_table)\
            .wait()

    except Exception as e:
       print("Writing records from Kafka to JDBC failed:", str(e)) 

if __name__ == '__main__':
    sessionize_events()

