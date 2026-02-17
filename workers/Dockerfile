FROM python:3.11-slim

WORKDIR /app

# Install dependencies
RUN pip install --no-cache-dir redis asyncpg httpx

# Copy worker code
COPY worker.py .

# Run the worker
CMD ["python", "worker.py"]
