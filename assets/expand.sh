#!/usr/bin/env bash
# Expand phase script (optional)
# Use environment variables:
#   ZDD_IS_HEAD: "true" if this is the latest deployment being applied
#   ZDD_DEPLOYMENT_ID: Current deployment ID
#   ZDD_DEPLOYMENT_NAME: Current deployment name
#   ZDD_PHASE: Current phase (expand/migrate/contract/post)
#   ZDD_DEPLOYMENTS_PATH: Path to deployments directory
#   ZDD_DATABASE_URL: Database connection string

set -e

# Example: Skip certain operations during catchup
# if [ "$ZDD_IS_HEAD" != "true" ]; then
#   echo "Skipping expand operations during catchup"
#   exit 0
# fi
