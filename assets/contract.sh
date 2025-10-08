#!/usr/bin/env bash
# Contract phase script (optional)
# Use environment variables:
#   ZDD_IS_HEAD: "true" if this is the latest migration being applied
#   ZDD_MIGRATION_ID: Current migration ID
#   ZDD_MIGRATION_NAME: Current migration name
#   ZDD_PHASE: Current phase (expand/migrate/contract/post)
#   ZDD_MIGRATIONS_PATH: Path to migrations directory
#   ZDD_DATABASE_URL: Database connection string

set -e
