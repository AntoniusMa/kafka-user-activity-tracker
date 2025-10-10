SELECT id, user_id, event_type, timestamp
FROM user_events
WHERE user_id = $1
ORDER BY timestamp DESC
