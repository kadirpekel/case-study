import os
import requests
import random
from datetime import datetime, timedelta
from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

MODELS_BACKEND_URL = os.getenv("MODELS_BACKEND_URL", "http://models-backend:8000")
NUM_WORKERS = 20
SUB_BATCH_SIZE = 10
MODELS = [f"model_{i}" for i in range(1, 11)]

default_args = {
    'owner': 'airflow',
    'retries': 0,
    # Requirement: "Each of the workers has a timeout limit of 30 minutes"
    'execution_timeout': timedelta(minutes=30),
}

with DAG(
    dag_id='current_state_workflow',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule=None,  # Triggered manually
    catchup=False,
    max_active_runs=1,
    max_active_tasks=25, # Allow all 20 mapped tasks to run efficiently
    tags=['case_study', 'current_state'],
) as dag:

    @task
    def get_unsolved_task_batches():
        """
        Step 1: Get all new unsolved tasks and break them into 20 equal batches.
        """
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        records = pg_hook.get_records("SELECT id FROM tasks WHERE status = 'unsolved'")
        
        if not records:
            print("No unsolved tasks found.")
            return []

        task_ids = [r[0] for r in records]
        
        # Requirement: Split into NUM_WORKERS (20) batches as stated in the case
        avg = len(task_ids) / float(NUM_WORKERS)
        batches = []
        last = 0.0
        while last < len(task_ids):
            batch = task_ids[int(last):int(last + avg)]
            if batch:
                batches.append(batch)
            last += avg
        
        print(f"Split {len(task_ids)} tasks into {len(batches)} batches.")
        return batches

    @task
    def process_batch(batch_ids):
        """
        Step 3-4: Process the batch in sub-batches of 10.
        """
        if not batch_ids:
            return
            
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        # Requirement: "Each process sends a batch of 10 tasks and then gets a new batch of 10"
        for i in range(0, len(batch_ids), SUB_BATCH_SIZE):
            sub_batch = batch_ids[i : i + SUB_BATCH_SIZE]
            
            placeholders = ','.join(['%s'] * len(sub_batch))
            records = pg_hook.get_records(
                f"SELECT id, prompt, model FROM tasks WHERE id IN ({placeholders})",
                parameters=tuple(sub_batch)
            )
            tasks = [{"prompt": r[1], "model": r[2]} for r in records]
            
            try:
                response = requests.post(
                    f"{MODELS_BACKEND_URL}/batch",
                    json={"items": tasks},
                    timeout=150 # Wait for backend (sth slightly upper than 2m)
                )
                response.raise_for_status()
                results = response.json()
                
                # Update status
                for idx, task_id in enumerate(sub_batch):
                    result = results[idx]
                    pg_hook.run(
                        "UPDATE tasks SET answer = %s, status = 'solved', model = %s WHERE id = %s",
                        parameters=(result['answer'], result['model'], task_id)
                    )
            except Exception as e:
                print(f"Error in sub-batch: {e}")

    # Requirement: "After all of the workers have solved their batch, the DAG restarts"
    restart_dag = TriggerDagRunOperator(
        task_id='restart_itself',
        trigger_dag_id='current_state_workflow',
        wait_for_completion=False,
        reset_dag_run=True,
        trigger_rule=TriggerRule.ALL_DONE,
    )

    batches = get_unsolved_task_batches()
    workers = process_batch.expand(batch_ids=batches)
    workers >> restart_dag
