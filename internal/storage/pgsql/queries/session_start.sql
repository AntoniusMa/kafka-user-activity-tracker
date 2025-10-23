INSERT INTO user_sessions (user_id, is_active)
VALUES ($1, $2)
RETURNING session_id, user_id, is_active, created_at, updated_at
