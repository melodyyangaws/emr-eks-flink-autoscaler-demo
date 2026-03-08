"""
Glue Catalog Integration Test for EMR on EKS + Flink
=====================================================
Tests: catalog creation, database creation, table DDL, datagen → print pipeline.
Based on: https://docs.aws.amazon.com/emr/latest/EMR-on-EKS-DevelopmentGuide/glue-for-flink.html

Usage:
  Upload to S3, then submit via FlinkDeployment (see glue-catalog-test.yaml).
"""
import logging
import sys

from pyflink.datastream import StreamExecutionEnvironment
from pyflink.table import StreamTableEnvironment


def glue_catalog_test():
    env = StreamExecutionEnvironment.get_execution_environment()
    t_env = StreamTableEnvironment.create(stream_execution_environment=env)

    # ------------------------------------------------------------------
    # 1. Create Glue-backed Hive catalog using EMR-provided conf dirs
    # ------------------------------------------------------------------
    print(">>> Step 1: Creating Glue-backed Hive catalog...")
    t_env.execute_sql("""
        CREATE CATALOG glue_catalog WITH (
            'type' = 'hive',
            'default-database' = 'default',
            'hive-conf-dir' = '/glue/confs/hive/conf',
            'hadoop-conf-dir' = '/glue/confs/hadoop/conf'
        )
    """)
    t_env.execute_sql("USE CATALOG glue_catalog")
    print(">>> Glue catalog created and activated.")

    # ------------------------------------------------------------------
    # 2. Create test database
    # ------------------------------------------------------------------
    print(">>> Step 2: Creating test database...")
    t_env.execute_sql("""
        CREATE DATABASE IF NOT EXISTS glue_test_db
        WITH ('hive.database.location-uri' =
              's3://${BUCKET_NAME}/flink/warehouse/glue_test_db/')
    """)
    t_env.execute_sql("USE glue_test_db")
    print(">>> Database 'glue_test_db' ready.")

    # ------------------------------------------------------------------
    # 3. Create datagen source table (generates random order data)
    # ------------------------------------------------------------------
    print(">>> Step 3: Creating datagen source table...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS datagen_orders (
            order_id       INT,
            customer_name  STRING,
            amount         DECIMAL(10, 2),
            order_time     TIMESTAMP(3),
            WATERMARK FOR order_time AS order_time - INTERVAL '5' SECOND
        ) WITH (
            'connector' = 'datagen',
            'rows-per-second' = '5',
            'fields.order_id.min' = '1',
            'fields.order_id.max' = '100000',
            'fields.amount.min' = '1',
            'fields.amount.max' = '999',
            'fields.customer_name.length' = '8'
        )
    """)
    print(">>> Source table 'datagen_orders' created.")

    # ------------------------------------------------------------------
    # 4. Create print sink table (outputs to TaskManager stdout)
    # ------------------------------------------------------------------
    print(">>> Step 4: Creating print sink table...")
    t_env.execute_sql("""
        CREATE TABLE IF NOT EXISTS print_orders (
            order_id       INT,
            customer_name  STRING,
            amount         DECIMAL(10, 2),
            order_time     TIMESTAMP(3)
        ) WITH (
            'connector' = 'print'
        )
    """)
    print(">>> Sink table 'print_orders' created.")

    # ------------------------------------------------------------------
    # 5. Verify tables are visible in Glue
    # ------------------------------------------------------------------
    print(">>> Step 5: Listing tables in glue_test_db...")
    result = t_env.execute_sql("SHOW TABLES")
    for row in result.collect():
        print(f"    Table: {row}")

    # ------------------------------------------------------------------
    # 6. Run a short streaming pipeline: datagen → print
    # ------------------------------------------------------------------
    print(">>> Step 6: Running datagen -> print pipeline (streaming)...")
    print(">>> Check TaskManager logs for output rows.")
    print(">>> The job will run until cancelled.")
    t_env.execute_sql("""
        INSERT INTO print_orders
        SELECT order_id, customer_name, amount, order_time
        FROM datagen_orders
    """)


if __name__ == "__main__":
    logging.basicConfig(stream=sys.stdout, level=logging.INFO, format="%(message)s")
    glue_catalog_test()
