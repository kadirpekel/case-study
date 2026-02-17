import os
import random
import json
import multiprocessing
import asyncio
import logging
from contextlib import asynccontextmanager
import redis.asyncio as redis
import asyncpg
import httpx

logging.basicConfig(level=logging.WARNING)
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6380/0")
MODELS_BACKEND_URL = os.getenv("MODELS_BACKEND_URL", "http://localhost:8000")
POSTGRES_URL = os.getenv("POSTGRES_URL", "postgresql://airflow:airflow@localhost:5433/airflow")

NUM_WORKERS = int(os.getenv("NUM_WORKERS", 8))
POOL_SIZE = int(os.getenv("POOL_SIZE", 50))

# Rate Limiting Configuration
RATE_LIMIT_TOKENS = 20    # Number of tasks allowed
RATE_LIMIT_WINDOW = 60    # Time frame in seconds (RPM = TOKENS / WINDOW * 60)
RATE_LIMIT_CAPACITY = 20   # Maximum burst (bucket size)

# Config per model
RATE_LIMIT_CONFIG = {f"model_{i}": RATE_LIMIT_TOKENS for i in range(1, 11)}

# Atomic rate limit + pop script
ATOMIC_RATE_AND_POP_SCRIPT = """
local queue_key = KEYS[1]
local rate_key_prefix = KEYS[2]
local refill_rate = tonumber(ARGV[1])   -- How many tokens added per second
local bucket_capacity = tonumber(ARGV[2]) -- Max tokens the bucket can hold
local requested = 1.0

-- 1. Get current time (server side)
local time = redis.call('TIME')
local now = tonumber(time[1]) + (tonumber(time[2]) / 1000000)

-- 2. Token Bucket State
local tokens_key = rate_key_prefix .. ":tokens"
local timestamp_key = rate_key_prefix .. ":ts"

local last_tokens = tonumber(redis.call("GET", tokens_key) or bucket_capacity)
local last_refreshed = tonumber(redis.call("GET", timestamp_key) or 0)

-- 3. Refill tokens
local delta = math.max(0, now - last_refreshed)
local current_tokens = math.min(bucket_capacity, last_tokens + (delta * refill_rate))

local allowed = false
local wait_time = 0

if current_tokens >= requested then
    allowed = true
    local remaining_tokens = current_tokens - requested
    
    -- update state
    redis.call("SET", tokens_key, remaining_tokens)
    redis.call("SET", timestamp_key, now)
    redis.call("EXPIRE", tokens_key, 60)
    redis.call("EXPIRE", timestamp_key, 60)
else
    allowed = false
    local needed = requested - current_tokens
    wait_time = needed / refill_rate
end

if not allowed then
    return {0, wait_time}
end

-- 4. Pop Task (only if rate limit allowed)
local task = redis.call('LPOP', queue_key)

if not task then
    -- Refund the token if no task was found
    local tokens_after_fail = tonumber(redis.call("GET", tokens_key) or 0)
    redis.call("SET", tokens_key, math.min(bucket_capacity, tokens_after_fail + requested))
    return {1, 0}
end

return {2, task}
"""


class Consumer:
    def __init__(self, postgres_pool, redis_pool, pool_size=None):
        self.postgres_pool = postgres_pool
        self.redis_pool = redis_pool
        self.pool_size = pool_size or POOL_SIZE

    async def run(self):
        logger.info(f"Worker process started with {self.pool_size} concurrent consumers.")
        # Start multiple consumer coroutines within this process
        consumers = [self.consume_loop(i) for i in range(self.pool_size)]
        await asyncio.gather(*consumers)

    async def consume_loop(self, consumer_id):
        client = redis.Redis(connection_pool=self.redis_pool)
        models = None
        
        logger.info(f"Consumer {consumer_id} started")
        
        while True:
            try:

                if not models:
                    models = list(RATE_LIMIT_CONFIG.keys())
                    random.shuffle(models)

                # Pick a random model to check until the list is empty
                model = models.pop()
                
                queue_key = f"queue:{model}"
                rate_key = f"rate:{model}"
                
                # 1. Fetch config for this model
                tokens_allowed = RATE_LIMIT_CONFIG[model]
                time_frame = RATE_LIMIT_WINDOW
                bucket_capacity = RATE_LIMIT_CAPACITY
                
                # 2. Calculate refill rate (tokens added per second)
                refill_rate = tokens_allowed / time_frame
                
                # 3. Request token + task atomically
                result = await client.eval(
                    ATOMIC_RATE_AND_POP_SCRIPT,
                    2,
                    queue_key,
                    rate_key,
                    refill_rate,
                    float(bucket_capacity)
                )
                
                status = result[0]
                
                if status == 0:
                    # Rate limited
                    wait_time = float(result[1])
                    await asyncio.sleep(wait_time)
                    continue
                    
                elif status == 1:
                    # No task available
                    await asyncio.sleep(0.1)
                    continue
                    
                else:
                    # Got task
                    task_json = result[1]
                    task = json.loads(task_json)
                    await self.process_task(task)
                    
            except Exception as e:
                logger.error(f"Error in consumer {consumer_id}: {e}")
                await asyncio.sleep(1)

    async def process_task(self, task):
        task_id = task['id']
        model = task['model']
        prompt = task['prompt']

        # Claim in database
        async with self.postgres_pool.acquire() as connection:
            claimed = await connection.fetchval(
                "UPDATE tasks SET status = 'processing' WHERE id = $1 AND status = 'queued' RETURNING id",
                task_id
            )

        if not claimed:
            return

        # Call backend
        try:
            async with httpx.AsyncClient() as client:
                response = await client.post(
                    f"{MODELS_BACKEND_URL}/solve",
                    json={"prompt": prompt, "model": model},
                    timeout=120
                )
                response.raise_for_status()
                answer = response.json().get("answer")

                async with self.postgres_pool.acquire() as connection:
                    await connection.execute(
                        "UPDATE tasks SET answer = $1, status = 'solved', model = $2 WHERE id = $3",
                        answer, model, task_id
                    )

                logger.info(f"Task {task_id} solved")

        except Exception as e:
            async with self.postgres_pool.acquire() as connection:
                await connection.execute(
                    "UPDATE tasks SET status = 'failed', error_message = $1 WHERE id = $2",
                    str(e)[:100], task_id
                )


@asynccontextmanager
async def create_redis_pool():
    pool = redis.ConnectionPool.from_url(REDIS_URL)
    try:
        yield pool
    finally:
        await pool.disconnect()


async def run_worker(worker_id):
    async with create_redis_pool() as redis_pool:
        async with asyncpg.create_pool(POSTGRES_URL) as postgres_pool:
            consumer = Consumer(postgres_pool, redis_pool)
            await consumer.run()


def start_worker_process(worker_id):
    try:
        asyncio.run(run_worker(worker_id))
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    logger.info(f"Spawning {NUM_WORKERS} worker processes...")

    processes = []
    for i in range(NUM_WORKERS):
        p = multiprocessing.Process(target=start_worker_process, args=(i,))
        p.start()
        processes.append(p)

    try:
        for p in processes:
            p.join()
    except KeyboardInterrupt:
        logger.info("Shutting down...")
        for p in processes:
            p.terminate()
            p.join()