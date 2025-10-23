SELECT session_id, user_id, is_active, created_at, updated_at
FROM user_sessions
WHERE session_id = $1
