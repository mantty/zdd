-- Add an index on email (contract phase - after code is updated to use email)
CREATE INDEX idx_users_email ON test_users(email);
