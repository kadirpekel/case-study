"""
Proposed Maintenance DAG — Handles failed task retry and cleanup.

Runs hourly. Reviews tasks that failed after multiple attempts
and either retries them or leaves them for manual review.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.providers.postgres.hooks.postgres import PostgresHook

MAX_RETRIES = 3

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'execution_timeout': timedelta(minutes=5),
}

with DAG(
    dag_id='proposed_maintenance',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule='*/10 * * * *',  # Every hour
    catchup=False,
    max_active_runs=1,
    tags=['case_study', 'proposed'],
) as dag:

    @task
    def retry_failed_tasks():
        """
        Re-queue failed tasks that haven't exceeded max retries.
        Tasks with attempt_count >= MAX_RETRIES stay as 'failed' for review.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        # Re-queue retryable failed tasks
        result = pg_hook.get_first("""
            WITH retried AS (
                UPDATE tasks
                SET status = 'unsolved',
                    error_message = NULL
                WHERE status = 'failed'
                  AND attempt_count < %(max_retries)s
                RETURNING id
            )
            SELECT count(*) FROM retried
        """, parameters={'max_retries': MAX_RETRIES})

        retried_count = result[0] if result else 0
        print(f"Re-queued {retried_count} failed tasks for retry.")

        # Count permanently failed tasks (exceeded max retries)
        permanent = pg_hook.get_first("""
            SELECT count(*) FROM tasks
            WHERE status = 'failed' AND attempt_count >= %(max_retries)s
        """, parameters={'max_retries': MAX_RETRIES})

        permanent_count = permanent[0] if permanent else 0
        if permanent_count > 0:
            print(f"⚠️  {permanent_count} tasks permanently failed (exceeded {MAX_RETRIES} retries).")

        return {
            "retried": retried_count,
            "permanently_failed": permanent_count,
        }

    @task
    def unstick_stale_tasks():
        """
        Safety net: reset tasks stuck in 'queued' or 'processing' status for too long.
        - 'queued': Dispacher claimed it but possibly lost during Redis push or worker crash.
        - 'processing': Worker claimed it but crashed during the model call.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        result = pg_hook.get_first("""
            WITH unstuck AS (
                UPDATE tasks
                SET status = 'unsolved'
                WHERE status IN ('queued', 'processing')
                  AND last_attempted_at < NOW() - INTERVAL '10 minutes'
                RETURNING id
            )
            SELECT count(*) FROM unstuck
        """)

        unstuck_count = result[0] if result else 0
        if unstuck_count > 0:
            print(f"Reset {unstuck_count} stale tasks back to 'unsolved'.")

        return {"unstuck": unstuck_count}

    @task
    def report_stats():
        """Print a summary of the task table for monitoring."""
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')

        stats = pg_hook.get_records("""
            SELECT status, count(*)
            FROM tasks
            GROUP BY status
            ORDER BY status
        """)

        print("=== Task Table Stats ===")
        for status, count in stats:
            print(f"  {status}: {count}")
        print("========================")

        return {row[0]: row[1] for row in stats}

    # Flow: retry failed → unstick stale → report
    retry_failed_tasks() >> unstick_stale_tasks() >> report_stats()
