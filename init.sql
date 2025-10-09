-- Initialize user_activity database schema

CREATE TABLE IF NOT EXISTS users (
    user_id VARCHAR(255) PRIMARY KEY,
    first_name VARCHAR(255) NOT NULL,
    last_name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS user_events (
    id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
    user_id VARCHAR(255) NOT NULL,
    event_type VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    FOREIGN KEY (user_id) REFERENCES users(user_id) ON DELETE CASCADE
);

-- Create indexes for faster queries
CREATE INDEX IF NOT EXISTS idx_user_events_user_id ON user_events(user_id);
CREATE INDEX IF NOT EXISTS idx_user_events_timestamp ON user_events(timestamp);

-- Insert some test data (optional)
INSERT INTO users (user_id, first_name, last_name)
VALUES
    ('test-001', 'Billiam', 'Gates'),
    ('test-002', 'Den', 'Rasen')
ON CONFLICT (user_id) DO NOTHING;

-- Insert some test events
INSERT INTO user_events (user_id, event_type, timestamp)
VALUES
    ('test-001', 'LOGIN', NOW() - INTERVAL '1 hour'),
    ('test-001', 'PAGE-VIEWS', NOW() - INTERVAL '30 minutes'),
    ('test-002', 'USER-ACTION', NOW() - INTERVAL '15 minutes');
