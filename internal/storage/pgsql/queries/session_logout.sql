UPDATE user_sessions
SET is_active = false, updated_at = CURRENT_TIMESTAMP
WHERE user_id = $1 AND is_active = true
