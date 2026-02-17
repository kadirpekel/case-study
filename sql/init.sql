CREATE TABLE IF NOT EXISTS tasks (
    id SERIAL PRIMARY KEY,
    prompt TEXT NOT NULL,
    status VARCHAR(20) DEFAULT 'unsolved',
    answer TEXT,
    model VARCHAR(50) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,

    -- Additional fields
    priority INT DEFAULT 0,
    last_attempted_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    attempt_count INT DEFAULT 0,
    error_message TEXT
);

-- Index for fast "fetch pending" queries
CREATE INDEX IF NOT EXISTS idx_tasks_unsolved
    ON tasks(status, priority DESC, created_at)
    WHERE status = 'unsolved';

-- Deterministic Task Generation for Benchmarking
-- Total: 1000 tasks

TRUNCATE TABLE tasks RESTART IDENTITY;

-- Set explicit seed for reproducible randomness
SELECT setseed(0.12345);

INSERT INTO tasks (prompt, status, model, created_at, priority)
SELECT 
    'Translate this standard text fragment #' || i,
    'unsolved',
    'model_' || floor(random() * 10 + 1)::int, -- Random 1-10
    NOW() - (INTERVAL '1 second' * (1000 - i)), 
    10 
FROM generate_series(1, 1000) AS s(i);
