import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def gen_kafka_source(bootstrap_servers,consumer_group,startup_mode="earliest-offset"):
    return f"""
    CREATE TABLE orders_kafka_source (
      id INT,
      price DECIMAL(32,2),
      buyer ROW<first_name STRING, last_name STRING>,
      city STRING,
      order_time TIMESTAMP(3),
      WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'order-data',
      'properties.bootstrap.servers' = '{bootstrap_servers}',
      'properties.group.id' = '{consumer_group}',
      'scan.startup.mode' = '{startup_mode}',
      'format' = 'json'
    )"""

def gen_sink_s3(s3_path):
     return f"""
      CREATE TABLE orders_s3_sink (
      id INT,
      price DECIMAL(32,2),
      buyer ROW<first_name STRING, last_name STRING>,
      city STRING,
      order_time TIMESTAMP(3)
      )
      WITH (
      'connector' = 'filesystem',
      'path' = '{s3_path}', 
      'format' = 'parquet'
    )"""

def gen_sink_agg_kafka(bootstrap_servers):
    QUERY=f"""CREATE TABLE orders_kafka_agg_sink (
      city STRING,
      num BIGINT,
      window_start TIMESTAMP(3),
      window_end TIMESTAMP(3),
      PRIMARY KEY(city) NOT ENFORCED
    ) WITH (
      'connector' = 'upsert-kafka',
      'topic' = 'order-data-agg',
      'properties.bootstrap.servers' = '{bootstrap_servers}',
      'key.format'='json',
      'value.format'='json'
    )"""
    return QUERY

def sink_app(boostrap_servers,consumer_group,startup_mode,s3_path):
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    config = t_env.get_config().get_configuration()
    config.set_string("pipeline.name", "msk-sink-s3 and msk-sink-upsert-kafka")

    t_env.execute_sql(gen_kafka_source(boostrap_servers,consumer_group,startup_mode))
    t_env.execute_sql(gen_sink_s3(s3_path))
    t_env.execute_sql(gen_sink_agg_kafka(boostrap_servers))
    table_result = t_env.execute_sql("""
        EXECUTE STATEMENT SET 
        BEGIN
        INSERT INTO orders_s3_sink SELECT * FROM orders_kafka_source;
        INSERT INTO orders_kafka_agg_sink SELECT city, COUNT(1) AS num,TUMBLE_START(order_time, INTERVAL '30' SECOND) AS window_start,TUMBLE_END(order_time, INTERVAL '30' SECOND) AS window_end FROM orders_kafka_source GROUP BY TUMBLE(order_time, INTERVAL '30' SECOND),city, order_time HAVING order_time BETWEEN TUMBLE_START(order_time, INTERVAL '30' SECOND) AND TUMBLE_END(order_time, INTERVAL '30' SECOND) + INTERVAL '3' SECOND;
        END;
    """)
    
    # Monitor job execution
    try:
        job_client = table_result.get_job_client()
        if job_client:
            logging.info(f"Job submitted with ID: {job_client.get_job_id()}")
            job_client.get_job_execution_result().result()
        else:
            logging.error("Failed to obtain job client")
    except Exception as e:
        logging.exception("Job execution failed")
        raise

if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    if len(sys.argv) == 5:
      bootstrap_servers=sys.argv[1]
      consumer_group=sys.argv[2]
      startup_mode=sys.argv[3]
      s3_path=sys.argv[4]
      sink_app(bootstrap_servers,consumer_group,startup_mode,s3_path)
    else:
      logging.error("Incorrect number of params")
