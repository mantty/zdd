#!/usr/bin/env bash
# Migrate phase script (optional)
# Use environment variables:
#   ZDD_IS_HEAD: "true" if this is the latest deployment being applied
#   ZDD_DEPLOYMENT_ID: Current deployment ID
#   ZDD_DEPLOYMENT_NAME: Current deployment name
#   ZDD_PHASE: Current phase (expand/migrate/contract/post)
#   ZDD_DEPLOYMENTS_PATH: Path to deployments directory
#   ZDD_DATABASE_URL: Database connection string

set -e

# Example: Run data migrations
# psql "$ZDD_DATABASE_URL" -c "UPDATE users SET status = 'active' WHERE status IS NULL"
