INSERT INTO user_events (session_id, event_type, timestamp)
VALUES ($1, $2, $3)
RETURNING id, session_id, event_type, timestamp
