#!/usr/bin/env python3
"""
Generic Flink CDC SQL Executor
==============================

A flexible PyFlink application that executes SQL scripts dynamically for CDC pipelines.
Supports both Apache Paimon and Apache Iceberg lakehouse formats.

Features:
- Load SQL scripts from S3 or local files
- Template variable substitution (environment variables)
- Execute multiple SQL files in sequence
- StatementSet for multiple INSERT statements (required in Application mode)
- Support for both streaming and batch modes
- Catalog-agnostic (Paimon, Iceberg, or custom)

Usage:
  python flink-cdc-executor.py --scripts s3://bucket/scripts/
  python flink-cdc-executor.py --scripts /opt/flink/sql/ --format paimon
  python flink-cdc-executor.py --sql-file setup.sql,pipelines.sql
"""

import os
import sys
import argparse
import re
from pathlib import Path
from typing import List, Dict, Optional
from pyflink.table import EnvironmentSettings, TableEnvironment
from pyflink.table.table_result import TableResult

class FlinkCDCExecutor:
    """Generic Flink SQL CDC Executor"""

    def __init__(self,
                 execution_mode: str = 'streaming',
                 checkpoint_interval: str = '60s',
                 parallelism: int = 4):
        """
        Initialize Flink CDC Executor

        Args:
            execution_mode: 'streaming' or 'batch'
            checkpoint_interval: Checkpoint interval (e.g., '60s', '1min')
            parallelism: Default parallelism
        """
        self.execution_mode = execution_mode
        self.checkpoint_interval = checkpoint_interval
        self.parallelism = parallelism
        self.table_env = None
        self.env_vars = dict(os.environ)
        self.pending_inserts: List[str] = []

    def create_table_environment(self):
        """Create and configure Flink Table Environment"""
        print("=" * 80)
        print(f"Initializing Flink Table Environment ({self.execution_mode} mode)")
        print("=" * 80)

        if self.execution_mode == 'streaming':
            settings = EnvironmentSettings.new_instance() \
                .in_streaming_mode() \
                .build()
        else:
            settings = EnvironmentSettings.new_instance() \
                .in_batch_mode() \
                .build()

        self.table_env = TableEnvironment.create(settings)

        if self.execution_mode == 'streaming':
            self.table_env.get_config().set(
                "execution.checkpointing.interval",
                self.checkpoint_interval
            )
            self.table_env.get_config().set(
                "execution.checkpointing.mode",
                "EXACTLY_ONCE"
            )

        self.table_env.get_config().set(
            "parallelism.default",
            str(self.parallelism)
        )

        self.table_env.get_config().set(
            "table.exec.source.idle-timeout",
            "30s"
        )

        self.table_env.get_config().set(
            "table.dynamic-table-options.enabled",
            "true"
        )

        print(f"✓ Table environment initialized")
        print(f"  Mode: {self.execution_mode}")
        print(f"  Parallelism: {self.parallelism}")
        if self.execution_mode == 'streaming':
            print(f"  Checkpoint: {self.checkpoint_interval}")

        return self.table_env

    def substitute_variables(self, sql_content: str) -> str:
        """
        Substitute environment variables in SQL content

        Supports patterns:
        - ${VAR_NAME} or ${VAR_NAME:default}
        - #{VAR_NAME} or #{VAR_NAME:default}
        - {{VAR_NAME}} or {{VAR_NAME:default}}
        """
        def replace_var(match):
            var_name = match.group(1)
            default_value = match.group(2) if match.lastindex >= 2 else None

            value = self.env_vars.get(var_name)

            if value is None:
                if default_value is not None:
                    value = default_value
                else:
                    raise ValueError(
                        f"Environment variable '{var_name}' is not set and no default provided"
                    )

            return value

        patterns = [
            r'\$\{([A-Za-z0-9_]+)(?::([^}]*))?\}',
            r'#\{([A-Za-z0-9_]+)(?::([^}]*))?\}',
            r'\{\{([A-Za-z0-9_]+)(?::([^}]*))?\}\}'
        ]

        result = sql_content
        for pattern in patterns:
            result = re.sub(pattern, replace_var, result)

        return result

    def parse_sql_file(self, sql_content: str) -> List[str]:
        """
        Parse SQL file into individual statements

        Splits by semicolon, handles comments and multi-line statements
        """
        lines = []
        for line in sql_content.split('\n'):
            comment_pos = line.find('--')
            if comment_pos >= 0:
                line = line[:comment_pos]
            lines.append(line)

        sql_content = '\n'.join(lines)
        sql_content = re.sub(r'/\*.*?\*/', '', sql_content, flags=re.DOTALL)

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

        print(f"\n🚀 Submitting StatementSet...")
        result = stmt_set.execute()
        print(f"✓ StatementSet submitted successfully")

        return result

    def load_sql_file(self, file_path: str) -> str:
        """Load SQL content from file (local or S3)"""
        if file_path.startswith('s3://'):
            import boto3
            s3 = boto3.client('s3')

            parts = file_path.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            key = parts[1] if len(parts) > 1 else ''

            print(f"📥 Loading SQL from S3: s3://{bucket}/{key}")

            response = s3.get_object(Bucket=bucket, Key=key)
            content = response['Body'].read().decode('utf-8')

        else:
            print(f"📥 Loading SQL from file: {file_path}")

            with open(file_path, 'r') as f:
                content = f.read()

        return content

    def execute_sql_file(self, file_path: str):
        """
        Load and execute SQL file.

        DDL/SET statements execute immediately.
        INSERT statements are collected into pending_inserts for later StatementSet execution.
        """
        print("\n" + "=" * 80)
        print(f"Executing SQL File: {file_path}")
        print("=" * 80)

        sql_content = self.load_sql_file(file_path)
        sql_content = self.substitute_variables(sql_content)
        statements = self.parse_sql_file(sql_content)

        print(f"Found {len(statements)} SQL statements")

        for i, statement in enumerate(statements, 1):
            description = f"Statement {i}/{len(statements)}"
            self.execute_sql(statement, description)

    def execute_sql_files(self, file_paths: List[str]):
        """Execute multiple SQL files in sequence"""
        for file_path in file_paths:
            self.execute_sql_file(file_path)

    def execute_sql_directory(self, directory_path: str, pattern: str = "*.sql"):
        """Execute all SQL files in a directory (sorted alphabetically)"""
        if directory_path.startswith('s3://'):
            import boto3
            s3 = boto3.client('s3')

            parts = directory_path.replace('s3://', '').split('/', 1)
            bucket = parts[0]
            prefix = parts[1] if len(parts) > 1 else ''

            print(f"📂 Listing SQL files from S3: s3://{bucket}/{prefix}")

            response = s3.list_objects_v2(Bucket=bucket, Prefix=prefix)

            file_paths = []
            for obj in response.get('Contents', []):
                key = obj['Key']
                if key.endswith('.sql'):
                    file_paths.append(f"s3://{bucket}/{key}")

        else:
            print(f"📂 Listing SQL files from directory: {directory_path}")

            directory = Path(directory_path)
            file_paths = [str(f) for f in directory.glob(pattern)]

        file_paths.sort()

        print(f"Found {len(file_paths)} SQL file(s)")

        self.execute_sql_files(file_paths)

def main():
    """Main entry point"""
    parser = argparse.ArgumentParser(
        description='Generic Flink CDC SQL Executor',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  # Execute single SQL file
  python flink-cdc-executor.py --sql-file /opt/flink/sql/setup.sql

  # Execute multiple SQL files
  python flink-cdc-executor.py --sql-file setup.sql,pipelines.sql

  # Execute all SQL files in directory
  python flink-cdc-executor.py --sql-dir /opt/flink/sql/

  # Execute SQL files from S3
  python flink-cdc-executor.py --sql-dir s3://my-bucket/flink-sql/

  # Execute with custom configuration
  python flink-cdc-executor.py --sql-file setup.sql --mode batch --parallelism 8

Environment Variables:
  All environment variables are available for substitution in SQL files.
  Use ${VAR_NAME} or ${VAR_NAME:default} syntax.

  Common variables:
    MYSQL_HOST, MYSQL_PORT, MYSQL_USER, MYSQL_PASSWORD, MYSQL_DATABASE
    PAIMON_WAREHOUSE, ICEBERG_WAREHOUSE, GLUE_DATABASE
    AWS_REGION, LAKEHOUSE_FORMAT (paimon or iceberg)
        """
    )

    input_group = parser.add_mutually_exclusive_group(required=True)
    input_group.add_argument(
        '--sql-file',
        help='SQL file path(s) to execute (comma-separated for multiple)'
    )
    input_group.add_argument(
        '--sql-dir',
        help='Directory containing SQL files to execute'
    )

    parser.add_argument(
        '--mode',
        choices=['streaming', 'batch'],
        default='streaming',
        help='Execution mode (default: streaming)'
    )
    parser.add_argument(
        '--checkpoint-interval',
        default='60s',
        help='Checkpoint interval for streaming mode (default: 60s)'
    )
    parser.add_argument(
        '--parallelism',
        type=int,
        default=4,
        help='Default parallelism (default: 4)'
    )
    parser.add_argument(
        '--pattern',
        default='*.sql',
        help='File pattern for directory mode (default: *.sql)'
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Flink CDC SQL Executor")
    print("=" * 80)
    print(f"Execution Mode: {args.mode}")
    print(f"Parallelism: {args.parallelism}")
    if args.mode == 'streaming':
        print(f"Checkpoint Interval: {args.checkpoint_interval}")
    print("=" * 80)

    executor = FlinkCDCExecutor(
        execution_mode=args.mode,
        checkpoint_interval=args.checkpoint_interval,
        parallelism=args.parallelism
    )

    executor.create_table_environment()

    try:
        if args.sql_file:
            file_paths = [f.strip() for f in args.sql_file.split(',')]
            executor.execute_sql_files(file_paths)
        elif args.sql_dir:
            executor.execute_sql_directory(args.sql_dir, args.pattern)

        # Execute all collected INSERT statements as a single StatementSet
        result = executor.execute_pending_inserts()

        print("\n" + "=" * 80)
        print("✓ All SQL executed and StatementSet submitted!")
        if args.mode == 'streaming':
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
