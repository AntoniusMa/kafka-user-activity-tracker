UPDATE user_sessions
SET is_active = $2, updated_at = CURRENT_TIMESTAMP
WHERE session_id = $1
