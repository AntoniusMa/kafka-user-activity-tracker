SELECT session_id, user_id, is_active, created_at, updated_at
FROM user_sessions
WHERE user_id = $1 AND is_active = TRUE
ORDER BY created_at DESC
LIMIT 1
