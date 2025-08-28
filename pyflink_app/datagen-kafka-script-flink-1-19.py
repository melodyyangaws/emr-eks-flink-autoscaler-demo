import logging
import sys
from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment

def gen_source_query(rows_per_second):
    QUERY=f"""
    CREATE TABLE orders (
      id INT,
      price DECIMAL(32,2),
      buyer ROW<first_name STRING, last_name STRING>,
      city STRING,
      order_time AS LOCALTIMESTAMP
    )
    WITH (
      'connector' = 'datagen',
      'rows-per-second'='{rows_per_second}',
      'fields.id.kind'='random',
      'fields.id.min'='1',
      'fields.city.length'='2'
    );
    """
    return QUERY

def gen_sink_query(bootstrap_servers):
    QUERY=f"""CREATE TABLE orders_kafka_sink (
      id INT,
      price DECIMAL(32,2),
      buyer ROW<first_name STRING, last_name STRING>,
      city STRING,
      order_time TIMESTAMP(3)
    ) WITH (
      'connector' = 'kafka',
      'topic' = 'order-data',
      'properties.bootstrap.servers' = '{bootstrap_servers}',
      'format' = 'json'
    )"""
    return QUERY

def sink_kafka_app(rows_per_second,boostrap_servers):
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(env)
    configuration = t_env.get_config().get_configuration()
    configuration.set_string("pipeline.name", "datagen sink kafka")
    t_env.execute_sql(gen_source_query(rows_per_second))
    t_env.execute_sql(gen_sink_query(bootstrap_servers))
    t_env.execute_sql("""
          EXECUTE STATEMENT SET
          BEGIN
          INSERT INTO orders_kafka_sink SELECT * FROM orders;
          END;
    """)
if __name__ == '__main__':
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    if len(sys.argv) == 3:
      rows_per_second=int(sys.argv[1])
      bootstrap_servers=sys.argv[2]
      sink_kafka_app(rows_per_second,bootstrap_servers)
    else:
      logging.error("Incorrect number of params")
