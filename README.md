# ZDD - Zero Downtime Deployments

A opinionated CLI tool for managing SQL migrations and app deployments with PostgreSQL, designed around the expand-migrate-contract pattern for zero downtime deployments.

## Philosophy

ZDD has strong opinions about how migrations should be handled:

- **Migrations should be written in plain SQL** (no DSLs)
- **Migrations should be idempotent**
- **Deployments should be seamless**
- **Migrating should be fast**
- **We should be able to review migrations and their effects**
- **Migrations should be roll forward** - down migrations are risky. It's better to apply new migrations if required to revert
- **Migrations should be automatically wrapped in transactions**
- **PostgreSQL is good**
- **expand-migrate-contract is the proper way to handle deploying stateful apps**

## Features

- **Pre and Post SQL migrations**: Unlike most migration tools, ZDD allows both pre-deployment and post-deployment SQL
- **Expand-Migrate-Contract pattern**: Safely handle schema changes with zero downtime
- **Lexicographically sortable migration names**: Timestamp-based IDs ensure proper ordering
- **Transaction safety**: All SQL files are executed within transactions
- **Schema diffing**: Automatically generate before/after schema comparisons
- **Numbered SQL files**: Support for `pre.1.sql`, `pre.2.sql` for batching large changes
- **Environment variable configuration**: All settings can be configured via environment variables

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
| `--migrations-path` | `ZDD_MIGRATIONS_PATH` | Path to migrations directory (default: "migrations") |
| `--deploy-command` | `ZDD_DEPLOY_COMMAND` | Command to run for deployment (e.g., "npm deploy") |

### Commands

#### Create a new migration

```bash
zdd create add_users_table
```

This creates a new migration directory with timestamp-based ID:
```
migrations/
  20231004120000_add_users_table/
    pre.sql   # SQL to run before deployment
    post.sql  # SQL to run after deployment
```

#### List migrations

```bash
zdd list
```

Shows the status of all migrations:
```
Migration Status:
================

Applied (1):
  ✓ 20231004120000 - add_users_table (applied: 2023-10-04 12:05:30)

Pending (2):
  ○ 20231004130000 - add_posts_table [pre]
  ○ 20231004140000 - expand_contract_migration [pre+post]
```

#### Apply migrations

```bash
zdd migrate
```

Applies all pending migrations following the expand-migrate-contract pattern.

### Migration Flow

When you run `zdd migrate`, here's what happens:

1. **Check applied migrations** - Query the `zdd_migrations` schema to see what's already applied
2. **Validate outstanding migrations** - Ensure at most one migration has both pre and post SQL
3. **Dump current schema** - Capture the before state
4. **Apply regular migrations** - Apply all migrations up to the "head" migration (the one with both pre and post)
5. **Apply head migration** using expand-migrate-contract:
   - **Expand phase**: Apply pre-migration SQL (e.g., add nullable columns)
   - **Migrate phase**: Run the deploy command (app deployment)
   - **Contract phase**: Apply post-migration SQL (e.g., make columns required)
6. **Dump final schema** - Capture the after state
7. **Generate schema diff** - Show what changed

### Migration Examples

#### Simple Migration (only pre SQL)

```sql
-- migrations/20231004120000_add_users_table/pre.sql
CREATE TABLE users (
    id SERIAL PRIMARY KEY,
    email VARCHAR(255) UNIQUE NOT NULL,
    name VARCHAR(255) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
```

#### Expand-Contract Migration

```sql
-- migrations/20231004140000_add_email_column/pre.sql
-- Expand: Add new column as nullable first
ALTER TABLE users ADD COLUMN email_verified BOOLEAN DEFAULT FALSE;
```

```sql
-- migrations/20231004140000_add_email_column/post.sql  
-- Contract: Make it required after app deployment
ALTER TABLE users ALTER COLUMN email_verified SET NOT NULL;
```

#### Numbered SQL Files

For very large migrations, you can use numbered files:

```
migrations/20231004150000_large_migration/
  pre.1.sql   # First batch
  pre.2.sql   # Second batch  
  pre.3.sql   # Third batch
  post.1.sql  # Post-deployment batch 1
  post.2.sql  # Post-deployment batch 2
```

### Environment Setup

```bash
export ZDD_DATABASE_URL="postgres://user:password@localhost/mydb"
export ZDD_MIGRATIONS_PATH="./db/migrations"
export ZDD_DEPLOY_COMMAND="npm run deploy:production"

zdd migrate
```

### Database Schema

ZDD automatically creates a `zdd_migrations` schema to track applied migrations:

```sql
CREATE SCHEMA zdd_migrations;

CREATE TABLE zdd_migrations.applied_migrations (
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
