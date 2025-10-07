package postgres

import (
	"context"
	_ "embed"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/mantty/zdd"
)

//go:embed assets/setup_schema.sql
var createMigrationsTableSQL string

// DB wraps a PostgreSQL connection pool and implements zdd.DatabaseProvider
type DB struct {
	pool *pgxpool.Pool
	ctx  context.Context
}

// NewDB creates a new PostgreSQL database connection
func NewDB(ctx context.Context, databaseURL string) (*DB, error) {
	config, err := pgxpool.ParseConfig(databaseURL)
	if err != nil {
		return nil, fmt.Errorf("failed to parse database URL: %w", err)
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return nil, fmt.Errorf("failed to create connection pool: %w", err)
	}

	// Test connection
	if err := pool.Ping(ctx); err != nil {
		return nil, fmt.Errorf("failed to ping database: %w", err)
	}

	db := &DB{pool: pool, ctx: ctx}
	if err := db.InitMigrationSchema(); err != nil {
		pool.Close()
		return nil, err
	}

	return db, nil
}

// Close closes the database connection
func (db *DB) Close() error {
	db.pool.Close()
	return nil
}

// InitMigrationSchema creates the zdd_migrations schema and table if they don't exist
func (db *DB) InitMigrationSchema() error {
	_, err := db.pool.Exec(db.ctx, createMigrationsTableSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize migration schema: %w", err)
	}
	return nil
}

// GetAppliedMigrations returns all migrations that have been applied to the database
func (db *DB) GetAppliedMigrations() ([]zdd.DBMigrationRecord, error) {
	query := `
		SELECT id, name, applied_at, COALESCE(checksum, '') as checksum 
		FROM zdd_migrations.applied_migrations 
		ORDER BY applied_at ASC
	`

	rows, err := db.pool.Query(db.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied migrations: %w", err)
	}
	defer rows.Close()

	var migrations []zdd.DBMigrationRecord
	for rows.Next() {
		var m zdd.DBMigrationRecord
		if err := rows.Scan(&m.ID, &m.Name, &m.AppliedAt, &m.Checksum); err != nil {
			return nil, fmt.Errorf("failed to scan migration record: %w", err)
		}
		migrations = append(migrations, m)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating migration records: %w", err)
	}

	return migrations, nil
}

// GetLastAppliedMigration returns the most recently applied migration
func (db *DB) GetLastAppliedMigration() (*zdd.DBMigrationRecord, error) {
	query := `
		SELECT id, name, applied_at, COALESCE(checksum, '') as checksum 
		FROM zdd_migrations.applied_migrations 
		ORDER BY applied_at DESC 
		LIMIT 1
	`

	var m zdd.DBMigrationRecord
	err := db.pool.QueryRow(db.ctx, query).Scan(&m.ID, &m.Name, &m.AppliedAt, &m.Checksum)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No migrations applied yet
		}
		return nil, fmt.Errorf("failed to get last applied migration: %w", err)
	}

	return &m, nil
}

// RecordMigration records that a migration has been applied
func (db *DB) RecordMigration(migration zdd.Migration, checksum string) error {
	query := `
		INSERT INTO zdd_migrations.applied_migrations (id, name, applied_at, checksum)
		VALUES ($1, $2, NOW(), $3)
	`

	_, err := db.pool.Exec(db.ctx, query, migration.ID, migration.Name, checksum)
	if err != nil {
		return fmt.Errorf("failed to record migration %s: %w", migration.ID, err)
	}

	return nil
}

// ExecuteSQLInTransaction executes SQL statements within a transaction
func (db *DB) ExecuteSQLInTransaction(sqlStatements []string) error {
	tx, err := db.pool.Begin(db.ctx)
	if err != nil {
		return fmt.Errorf("failed to begin transaction: %w", err)
	}
	defer tx.Rollback(db.ctx) // Will be ignored if transaction is committed

	for i, sql := range sqlStatements {
		sql = strings.TrimSpace(sql)
		if sql == "" {
			continue
		}

		_, err := tx.Exec(db.ctx, sql)
		if err != nil {
			return fmt.Errorf("failed to execute SQL statement %d: %w", i+1, err)
		}
	}

	if err := tx.Commit(db.ctx); err != nil {
		return fmt.Errorf("failed to commit transaction: %w", err)
	}

	return nil
}

// DumpSchema exports the current database schema
func (db *DB) DumpSchema() (string, error) {
	// Simple schema dump - gets table definitions
	query := `
		SELECT t.table_schema, t.table_name,
		       'CREATE TABLE ' || t.table_schema || '.' || t.table_name || ' (' ||
		       array_to_string(
		           array_agg(c.column_name || ' ' || c.data_type ORDER BY c.ordinal_position),
		           ', '
		       ) || ');' AS table_def
		FROM information_schema.tables t
		JOIN information_schema.columns c
		  ON t.table_name = c.table_name
		 AND t.table_schema = c.table_schema
		WHERE t.table_schema NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		GROUP BY t.table_schema, t.table_name
		ORDER BY t.table_schema, t.table_name
	`

	rows, err := db.pool.Query(db.ctx, query)
	if err != nil {
		return "", fmt.Errorf("failed to dump schema: %w", err)
	}
	defer rows.Close()

	var schemaDump strings.Builder
	schemaDump.WriteString("-- Schema dump generated by zdd\n\n")

	for rows.Next() {
		var schema, table, tableDef string
		if err := rows.Scan(&schema, &table, &tableDef); err != nil {
			return "", fmt.Errorf("failed to scan table definition: %w", err)
		}

		schemaDump.WriteString(fmt.Sprintf("-- Table: %s.%s\n", schema, table))
		schemaDump.WriteString(tableDef)
		schemaDump.WriteString("\n\n")
	}

	if err := rows.Err(); err != nil {
		return "", fmt.Errorf("error iterating schema dump: %w", err)
	}

	return schemaDump.String(), nil
}
