#!/usr/bin/env python3
"""
Flink SQL executor for the throughput benchmark's two writers
==============================================================

Runs one format's SQL from S3 as a single streaming job: every .sql file under
--sql-dir, in name order (00-source, 02-catalog-setup, 03-sink, 04-pipeline).
Both FlinkDeployments (flink-cid-{paimon,iceberg}.yaml) start it the same way:

  flink-cid-executor.py --sql-dir s3://<bucket>/flink/cid-sql/<fmt>/ \\
      --checkpoint-interval 60s --parallelism <JOB_PARALLELISM>

- ${VAR} / ${VAR:default} in the SQL are filled from the environment (the
  manifests set them); a missing VAR with no default is an error.
- SET statements go to the table config; DDL runs at once; INSERTs are
  collected and submitted together as ONE StatementSet (Application mode
  allows a single execute()).

Started as a copy of cdc-pipeline's generic executor, trimmed (2026-10-20) to
what these jobs use: streaming only, an S3 directory only, ${...} only.
"""

import argparse
import os
import re
import sys
from typing import List, Optional

import boto3
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.table_result import TableResult


class FlinkSqlExecutor:
    """Execute a directory of SQL files as one streaming Flink job."""

    def __init__(self, checkpoint_interval: str = '60s', parallelism: int = 4):
        self.checkpoint_interval = checkpoint_interval
        self.parallelism = parallelism
        self.table_env = None
        self.env_vars = dict(os.environ)
        self.pending_inserts: List[str] = []

    def create_table_environment(self):
        """Streaming table environment: exactly-once checkpoints, the given parallelism."""
        print("=" * 80)
        print("Initializing Flink Table Environment (streaming mode)")
        print("=" * 80)

        self.table_env = TableEnvironment.create(EnvironmentSettings.new_instance().in_streaming_mode().build())
        config = self.table_env.get_config()
        config.set("execution.checkpointing.interval", self.checkpoint_interval)
        config.set("execution.checkpointing.mode", "EXACTLY_ONCE")
        config.set("parallelism.default", str(self.parallelism))
        config.set("table.exec.source.idle-timeout", "30s")
        config.set("table.dynamic-table-options.enabled", "true")

        print("✓ Table environment initialized")
        print(f"  Parallelism: {self.parallelism}")
        print(f"  Checkpoint: {self.checkpoint_interval}")
        return self.table_env

    def substitute_variables(self, sql_content: str) -> str:
        """Fill ${VAR_NAME} or ${VAR_NAME:default} from the environment."""
        def replace_var(match):
            var_name, default_value = match.group(1), match.group(2)
            value = self.env_vars.get(var_name)
            if value is None:
                if default_value is None:
                    raise ValueError(f"Environment variable '{var_name}' is not set and no default provided")
                value = default_value
            return value

        return re.sub(r'\$\{([A-Za-z0-9_]+)(?::([^}]*))?\}', replace_var, sql_content)

    def parse_sql_file(self, sql_content: str) -> List[str]:
        """
        Split SQL into statements on ';' line ends, dropping -- comments and
        /* block comments */ (but not /*+ hints */).
        """
        lines = []
        for line in sql_content.split('\n'):
            comment_pos = line.find('--')
            if comment_pos >= 0:
                line = line[:comment_pos]
            lines.append(line)

        sql_content = '\n'.join(lines)
        # Strip /* block comments */ but NOT /*+ hints */. Flink dynamic table
        # options (INSERT INTO t /*+ OPTIONS('k'='v') */ ...) are hints, not
        # comments, and share the /* opening — a blanket strip silently drops
        # them and the statement still executes, just without the options.
        # The negative lookahead on '+' keeps hints intact.
        sql_content = re.sub(r'/\*(?!\+).*?\*/', '', sql_content, flags=re.DOTALL)

        statements = []
        current_statement = []

        for line in sql_content.split('\n'):
            line = line.strip()
            if not line:
                continue

            current_statement.append(line)

            if line.endswith(';'):
                statement = ' '.join(current_statement).strip()
                if statement and statement != ';':
                    statement = statement.rstrip(';').strip()
                    statements.append(statement)
                current_statement = []

        if current_statement:
            statement = ' '.join(current_statement).strip()
            if statement:
                statements.append(statement)

        return statements

    def execute_sql(self, sql: str, description: str = None) -> Optional[TableResult]:
        """
        Execute a single SQL statement.

        - SET statements are routed to the config API.
        - INSERT statements are deferred to a StatementSet (collected, not executed yet).
        - All other DDL/DML statements are executed immediately.
        """
        if description:
            print(f"\n📝 {description}")

        try:
            sql_preview = sql[:200] + "..." if len(sql) > 200 else sql
            print(f"   SQL: {sql_preview}")

            sql_stripped = sql.strip()
            sql_upper = sql_stripped.upper()

            # Handle SET statements via configuration API (not supported by executeSql)
            if sql_upper.startswith("SET "):
                match = re.match(r"SET\s+'([^']+)'\s*=\s*'([^']+)'", sql_stripped, re.IGNORECASE)
                if match:
                    key, value = match.group(1), match.group(2)
                    self.table_env.get_config().set(key, value)
                    print(f"   ✓ Configuration set: {key} = {value}")
                    return None
                raise ValueError(f"Invalid SET syntax: {sql}")

            # Defer INSERT statements to StatementSet
            if sql_upper.startswith("INSERT "):
                self.pending_inserts.append(sql_stripped)
                print(f"   ✓ Deferred to StatementSet ({len(self.pending_inserts)} pending)")
                return None

            result = self.table_env.execute_sql(sql_stripped)
            print("   ✓ Executed successfully")
            return result

        except Exception as e:
            print(f"   ❌ Error executing SQL: {e}")
            print(f"   Full SQL:\n{sql}")
            raise

    def execute_pending_inserts(self):
        """
        Execute all pending INSERT statements as a single StatementSet.

        In Application mode, Flink only allows one execute()/executeAsync() call.
        A StatementSet batches multiple INSERTs into a single execution.
        """
        if not self.pending_inserts:
            print("\n⚠ No INSERT statements to execute")
            return None

        print("\n" + "=" * 80)
        print(f"Executing StatementSet with {len(self.pending_inserts)} INSERT statement(s)")
        print("=" * 80)

        stmt_set = self.table_env.create_statement_set()

        for i, sql in enumerate(self.pending_inserts, 1):
            sql_preview = sql[:120] + "..." if len(sql) > 120 else sql
            print(f"  [{i}] {sql_preview}")
            stmt_set.add_insert_sql(sql)

        print("\n🚀 Submitting StatementSet...")
        result = stmt_set.execute()
        print("✓ StatementSet submitted successfully")

        return result

    def execute_sql_file(self, s3, bucket: str, key: str):
        """Load one SQL file from S3, fill its variables and execute its statements."""
        print("\n" + "=" * 80)
        print(f"Executing SQL File: s3://{bucket}/{key}")
        print("=" * 80)

        sql_content = s3.get_object(Bucket=bucket, Key=key)['Body'].read().decode('utf-8')
        statements = self.parse_sql_file(self.substitute_variables(sql_content))

        print(f"Found {len(statements)} SQL statements")
        for i, statement in enumerate(statements, 1):
            self.execute_sql(statement, f"Statement {i}/{len(statements)}")

    def execute_sql_directory(self, directory_path: str):
        """Execute every .sql file under an s3:// prefix, in name order."""
        if not directory_path.startswith('s3://'):
            raise ValueError(f"--sql-dir must be an s3:// prefix, got '{directory_path}'")
        bucket, _, prefix = directory_path[len('s3://'):].partition('/')
        s3 = boto3.client('s3')

        print(f"📂 Listing SQL files from S3: s3://{bucket}/{prefix}")
        keys = sorted(obj['Key'] for obj in s3.list_objects_v2(Bucket=bucket, Prefix=prefix).get('Contents', [])
                      if obj['Key'].endswith('.sql'))
        print(f"Found {len(keys)} SQL file(s)")

        for key in keys:
            self.execute_sql_file(s3, bucket, key)


def main():
    parser = argparse.ArgumentParser(
        description="Run a directory of Flink SQL from S3 as one streaming job "
                    "(the throughput benchmark's writers).")
    parser.add_argument('--sql-dir', required=True,
                        help='s3:// prefix holding the .sql files, run in name order')
    parser.add_argument('--checkpoint-interval', default='60s',
                        help='Checkpoint interval (default: 60s)')
    parser.add_argument('--parallelism', type=int, default=4,
                        help='Default parallelism (default: 4)')
    args = parser.parse_args()

    print("=" * 80)
    print("Flink SQL Executor (throughput benchmark)")
    print("=" * 80)
    print(f"Parallelism: {args.parallelism}")
    print(f"Checkpoint Interval: {args.checkpoint_interval}")
    print("=" * 80)

    executor = FlinkSqlExecutor(checkpoint_interval=args.checkpoint_interval, parallelism=args.parallelism)
    executor.create_table_environment()

    try:
        executor.execute_sql_directory(args.sql_dir)

        # Execute all collected INSERT statements as a single StatementSet
        executor.execute_pending_inserts()

        print("\n" + "=" * 80)
        print("✓ All SQL executed and StatementSet submitted!")
        print("  Streaming job is running. Flink framework manages lifecycle.")
        print("  Do NOT call result.wait() in Application mode — it kills the job")
        print("  on transient failures (heartbeat timeouts, slot reallocations).")
        print("=" * 80)

    except KeyboardInterrupt:
        print("\n⚠ Interrupted by user")
        sys.exit(0)
    except Exception as e:
        print(f"\n❌ Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
