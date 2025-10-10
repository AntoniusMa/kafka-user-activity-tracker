INSERT INTO user_events (user_id, event_type, timestamp)
VALUES ($1, $2, $3)
RETURNING id, user_id, event_type, timestamp
