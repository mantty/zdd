#!/usr/bin/env bash
# Expand phase script (optional)
# Use environment variables:
#   ZDD_IS_HEAD: "true" if this is the latest migration being applied
#   ZDD_MIGRATION_ID: Current migration ID
#   ZDD_MIGRATION_NAME: Current migration name
#   ZDD_PHASE: Current phase (expand/migrate/contract/post)
#   ZDD_MIGRATIONS_PATH: Path to migrations directory

set -e

# Example: Skip certain operations during catchup
# if [ "$ZDD_IS_HEAD" != "true" ]; then
#   echo "Skipping expand operations during catchup"
#   exit 0
# fi
