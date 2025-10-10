# ZDD - Zero Downtime Deployments

A opinionat| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--database-url` | `ZDD_DATABASE_URL` | PostgreSQL connection string |
| `--deployments-path` | `ZDD_DEPLOYMENTS_PATH` | Path to deployments directory (default: "migrations") |LI tool for managing SQL migrations and app deployments with PostgreSQL, designed around the expand-migrate-contract pattern for zero downtime deployments.

## Philosophy

ZDD has the following opinions about how deployments should be handled:

- **Migrations should be written in plain SQL** (no DSLs)
- **Migrations should be idempotent**
- **Deployments should be seamless**
- **We should be able to review migrations and their effects**
- **Migrations should be roll forward** - down migrations are risky. It's better to apply new migrations if required to revert
- **Migrations should be automatically wrapped in transactions**
- **PostgreSQL is good**
- **expand-migrate-contract is the proper way to deploy stateful apps**

## Features

- **Shell script hooks**: Optional shell scripts (expand.sh, migrate.sh, contract.sh, post.sh) with environment variables for deployment control
- **Expand-Migrate-Contract pattern**: Safely handle schema changes with zero downtime
- **Transaction safety**: All SQL files are executed within transactions
- **Schema diffing**: Automatically generate before/after schema comparisons
- **Numbered SQL files**: Support for `expand.1.sql`, `expand.2.sql` ... `expand.n.sql` for batching large changes
- **Sequential migration IDs**: Conflict-free 6-digit migration identifiers

## Installation

```bash
go install github.com/mantty/zdd/cmd/zdd@latest
```

Or build from source:

```bash
git clone https://github.com/mantty/zdd.git
cd zdd
go build ./cmd/zdd
```

## Usage

### Configuration

ZDD can be configured via command line flags or environment variables:

| Flag | Environment Variable | Description |
|------|---------------------|-------------|
| `--database-url` | `ZDD_DATABASE_URL` | PostgreSQL connection string |
| `--deployments-path` | `ZDD_DEPLOYMENTS_PATH` | Path to deployments directory (default: "migrations") |

### Commands

#### Create a new deployment

```bash
zdd create add_users_table
```

This creates a new deployment directory with a sequential ID:
```
migrations/
  000001_add_users_table/
    expand.sh
    expand.sql
    migrate.sh
    migrate.sql
    contract.sh
    contract.sql
    post.sh
```

Each of the above files are optional and can be safely deleted.
Any deployment stage can have a script, an SQL migration, both, or neither.

#### List deployments

```bash
zdd list
```

Shows the status of all deployments:
```
Deployment Status:
==================

Applied (1):
  ✓ 000001 - add_users_table (applied: 2023-10-04 12:05:30)

Pending (2):
  ○ 000002 - add_posts_table
  ○ 000003 - expand_contract_deployment
```

#### Apply deployments

```bash
zdd deploy
```

Applies all pending deployments following the expand-migrate-contract pattern.


### Deployment Examples

#### Simple Deployment (only migrate SQL)

```sql
-- migrations/000001_add_users_table/migrate.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

#### Expand-Contract Deployment

```sql
-- migrations/000002_add_email_column/expand.sql
-- Expand: Add new column as nullable first
ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
```

```sql
-- migrations/000002_add_email_column/contract.sql  
-- Contract: Make it required after app deployment
ALTER TABLE users ALTER COLUMN email_verified SET NOT NULL;
```

#### Numbered SQL Files

For very large deployments, you can use numbered files:

```
migrations/000003_large_deployment/
  expand.1.sql    # First batch
  expand.2.sql    # Second batch  
  expand.3.sql    # Third batch
  migrate.sql     # Standalone migrate step
  contract.1.sql  # Post-deployment batch 1
  contract.2.sql  # Post-deployment batch 2
```

### Environment Setup

```bash
export ZDD_DATABASE_URL="postgres://user:password@localhost/mydb"
export ZDD_DEPLOYMENTS_PATH="./db/migrations"

zdd deploy
```

### Database Schema

ZDD automatically creates a `zdd_deployments` schema to track applied deployments:

```sql
CREATE SCHEMA zdd_deployments;

CREATE TABLE zdd_deployments.applied_deployments (
    id VARCHAR(255) PRIMARY KEY,
    name VARCHAR(500) NOT NULL,
    applied_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
    checksum VARCHAR(64)
);
```

## Contributing

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Add tests if applicable
5. Submit a pull request
