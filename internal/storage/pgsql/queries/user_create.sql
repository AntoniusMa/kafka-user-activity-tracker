INSERT INTO users (user_id, first_name, last_name)
VALUES ($1, $2, $3)
RETURNING user_id, first_name, last_name
