import os
import logging
import json
import redis
from datetime import datetime, timedelta

from airflow import DAG
from airflow.decorators import task
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)

# Configuration
REDIS_URL = os.getenv("AIRFLOW__CELERY__BROKER_URL", "redis://:@redis:6379/0")

NUM_TASKS = 1000  # Batch size to fetch from DB
MAX_QUEUE_SIZE = 10000  # Max tasks in Redis before backpressure kicks in, for 10 models this is 1000 tasks per model where it might occupy 10000 * 1000 * 10 bytes = 100MB of memory
MODEL_LIST = [f"model_{i}" for i in range(1, 11)]

redis_client = redis.Redis.from_url(REDIS_URL, decode_responses=True)

default_args = {
    'owner': 'airflow',
    'retries': 0,
    'execution_timeout': timedelta(minutes=10),
}

def get_queue(model):
    return f"queue:{model}"

def get_available_models():
    model_queues = [get_queue(model) for model in MODEL_LIST]
    
    pipeline = redis_client.pipeline()
    for model_queue in model_queues:
        pipeline.llen(model_queue)
    results = pipeline.execute()
    
    capacities = dict(zip(MODEL_LIST, results))
    logger.info(f"Model capacities: {capacities}")
    
    return [model for model, capacity in capacities.items() if capacity < MAX_QUEUE_SIZE]

with DAG(
    dag_id='proposed_producer',
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule="*/1 * * * *",  # Run every minute (or faster if needed)
    catchup=False,
    max_active_runs=1,
    tags=['case_study', 'producer'],
) as dag:

    @task
    def queue_tasks():

        # Get available models, we're using this for backpressure
        available_models = get_available_models()
        
        if not available_models:
            logger.info("No models allow tasks (all queues full).")
            return

        # Fetch pending tasks for available models
        pg_hook = PostgresHook(postgres_conn_id='postgres_default')
        
        records = pg_hook.get_records("""
            UPDATE tasks
            SET status = 'queued',
                last_attempted_at = NOW(),
                attempt_count = attempt_count + 1
            WHERE id IN (
                SELECT id FROM tasks
                WHERE status = 'unsolved' 
                  AND model = ANY(%(available_models)s)
                  AND attempt_count < 3
                ORDER BY priority DESC, created_at ASC
                LIMIT %(limit)s
                FOR UPDATE SKIP LOCKED
            )
            RETURNING id, prompt, model
        """, parameters={'limit': NUM_TASKS, 'available_models': available_models})
        
        if not records:
            logger.info("No unsolved tasks found in DB.")
            return

        # Push to Redis (Pipeline for performance)
        pipeline = redis_client.pipeline()
        count = 0
        for row in records:
            pipeline.rpush(get_queue(row[2]), json.dumps({
                "id": row[0],
                "prompt": row[1],
                "model": row[2]
            }))
            count += 1
        
        pipeline.execute()
        logger.info(f"Queued {count} tasks to Redis.")

    queue_tasks()
