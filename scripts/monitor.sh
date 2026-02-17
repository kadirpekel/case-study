#!/bin/bash

echo "Resetting all tasks to 'unsolved'..."
docker exec -i ebay-case-postgres-1 psql -U airflow -d airflow -c "UPDATE tasks SET status = 'unsolved', attempt_count = 0, last_attempted_at = NULL, error_message = NULL, answer = NULL;"
echo "Reset complete."
echo "------------------------------------------------"

# Monitor the task queue in real-time
echo "Monitoring task queue status... (Ctrl+C to stop)"
echo "------------------------------------------------"

#!/bin/bash
# Usage: ./scripts/collect_metrics.sh [scenario_name]

SCENARIO=${1:-default}
OUTPUT_FILE="metrics_${SCENARIO}.csv"

echo "timestamp,unsolved,queued,processing,solved,failed" > $OUTPUT_FILE
echo "Collecting metrics to $OUTPUT_FILE... (Press Ctrl+C to stop)"

while true; do
    TS=$(date +%s)
    
    # Run SQL to get counts
    # Output format: status | count
    # We use -A (unaligned) to ensure standard pipe separator
    DATA=$(docker exec -i $(docker ps -qf "name=postgres") psql -U airflow -d airflow -t -A -c "SELECT status, count(*) FROM tasks GROUP BY status;")
    
    # OUTPUT looks like:
    # unsolved|1000
    # processing|5
    
    # Parse counts (using cut with delimiter |)
    # Use anchor ^ to prevent substring matching (e.g. grep "solved" matches "unsolved")
    UNSOLVED=$(echo "$DATA" | grep "^unsolved|" | cut -d'|' -f2 | tr -d '\n')
    QUEUED=$(echo "$DATA" | grep "^queued|" | cut -d'|' -f2 | tr -d '\n')
    PROCESSING=$(echo "$DATA" | grep "^processing|" | cut -d'|' -f2 | tr -d '\n')
    SOLVED=$(echo "$DATA" | grep "^solved|" | cut -d'|' -f2 | tr -d '\n')
    FAILED=$(echo "$DATA" | grep "^failed|" | cut -d'|' -f2 | tr -d '\n')
    
    # Zero fill
    UNSOLVED=${UNSOLVED:-0}
    QUEUED=${QUEUED:-0}
    PROCESSING=${PROCESSING:-0}
    SOLVED=${SOLVED:-0}
    FAILED=${FAILED:-0}
    
    echo "$TS,$UNSOLVED,$QUEUED,$PROCESSING,$SOLVED,$FAILED" >> $OUTPUT_FILE
    
    # Optional: Live feedback
    echo "[$TS] Unsolved: $UNSOLVED | Queue: $QUEUED | Processing: $PROCESSING | Solved: $SOLVED"
    
    sleep 1
done