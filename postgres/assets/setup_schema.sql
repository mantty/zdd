CREATE SCHEMA IF NOT EXISTS zdd_deployments;

CREATE TABLE IF NOT EXISTS zdd_deployments.applied_deployments (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    checksum VARCHAR(64)
);

CREATE INDEX IF NOT EXISTS idx_applied_deployments_applied_at
    ON zdd_deployments.applied_deployments(applied_at);
