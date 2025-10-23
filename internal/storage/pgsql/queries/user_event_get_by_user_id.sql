SELECT e.id, e.session_id, e.event_type, e.timestamp
FROM user_events e
JOIN user_sessions s ON e.session_id = s.session_id
WHERE s.user_id = $1
ORDER BY e.timestamp DESC
