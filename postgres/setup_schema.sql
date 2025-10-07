CREATE SCHEMA IF NOT EXISTS zdd_migrations;

CREATE TABLE IF NOT EXISTS zdd_migrations.applied_migrations (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    checksum VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_applied_migrations_applied_at
    ON zdd_migrations.applied_migrations(applied_at);
