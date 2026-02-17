import time
import random
import threading

capacity = 5
refill_rate = 3
refill_interval = 1

store = {}
lock = threading.Lock()

def sliding_window_counter(id):
    interval = 5  # 5 second window
    with lock:
        now = time.time()
        curr_window = int(now / interval)
        prev_window = curr_window - 1
        
        # Keys for storage
        curr_key = f"{id}:{curr_window}"
        prev_key = f"{id}:{prev_window}"
        
        curr_count = store.get(curr_key, 0)
        prev_count = store.get(prev_key, 0)
        
        # Weight of the previous window (how much it overlaps with our 5s window)
        # Weight = (window_end - now) / window_size
        percentage_elapsed = (now % interval) / interval
        weight = 1 - percentage_elapsed
        
        estimated_count = curr_count + (prev_count * weight)
        
        if estimated_count < capacity:
            store[curr_key] = curr_count + 1
            # Optional: Clean up old keys here to prevent memory leaks
            return True
        return False

def sliding_window_counter(id):
    interval = 5

    with lock:
        now = time.time()
        count_key = f"count:{id}"
        time_key = f"time:{id}"
        prev_time = store.get(time_key, 0)
        next_boundary = prev_time + interval + interval
        w = (next_boundary - now) / interval
        count = store.get(count_key, 0)
        rate = ((1 - w) * capacity) + count
        if rate <= capacity:
            store[count_key] = rate + 1
            store[time_key] = prev_time + interval
            return True
        else:
            return False
        



def token_bucket(id):
    with lock:
        val = store.get(id)

        if not val:
            store[id] = {
                "tokens": capacity,
                "last_refill": time.time()
            }
            val = store[id]
        
        now = time.time()
        if val["last_refill"] < now - refill_interval:
            val["tokens"] = min(capacity, val["tokens"] + (now - val["last_refill"]) * refill_rate)
            val["last_refill"] = now
        
        if val["tokens"] >= 1:
            val["tokens"] -= 1
            store[id] = val
            return True
        else:
            return False

def main():

    while True:
        for i in range(5):
            if token_bucket(i):
                print(f"Request {i} is allowed")
            else:
                print(f"Request {i} is not allowed")
            time.sleep(random.uniform(0.01, 0.05))

if __name__ == "__main__":
    main()