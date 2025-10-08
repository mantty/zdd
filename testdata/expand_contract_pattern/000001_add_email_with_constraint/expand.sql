-- Create a base table (this would normally exist already)
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL
);

-- Add a nullable email column (expand phase - safe to add while app is running)
ALTER TABLE test_users ADD COLUMN email VARCHAR(255);
