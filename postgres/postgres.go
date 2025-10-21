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

type (
	// DB wraps a PostgreSQL connection pool and implements zdd.DatabaseProvider
	DB struct {
		pool    *pgxpool.Pool
		ctx     context.Context
		connStr string
	}
)

//go:embed assets/setup_schema.sql
var createDeploymentsTableSQL string

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

	db := &DB{
		pool:    pool,
		ctx:     ctx,
		connStr: databaseURL,
	}
	if err := db.InitDeploymentSchema(); err != nil {
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

// ConnectionString returns the database connection string
func (db *DB) ConnectionString() string {
	return db.connStr
}

// InitDeploymentSchema creates the zdd_deployments schema and table if they don't exist
func (db *DB) InitDeploymentSchema() error {
	_, err := db.pool.Exec(db.ctx, createDeploymentsTableSQL)
	if err != nil {
		return fmt.Errorf("failed to initialize deployment schema: %w", err)
	}
	return nil
}

// GetAppliedDeployments returns all deployments that have been applied to the database
func (db *DB) GetAppliedDeployments() ([]zdd.DBDeploymentRecord, error) {
	query := `
		SELECT id, name, applied_at, COALESCE(checksum, '') as checksum 
		FROM zdd_deployments.applied_deployments 
		ORDER BY applied_at ASC
	`

	rows, err := db.pool.Query(db.ctx, query)
	if err != nil {
		return nil, fmt.Errorf("failed to query applied deployments: %w", err)
	}
	defer rows.Close()

	var deployments []zdd.DBDeploymentRecord
	for rows.Next() {
		var d zdd.DBDeploymentRecord
		if err := rows.Scan(&d.ID, &d.Name, &d.AppliedAt, &d.Checksum); err != nil {
			return nil, fmt.Errorf("failed to scan deployment record: %w", err)
		}
		deployments = append(deployments, d)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("error iterating deployment records: %w", err)
	}

	return deployments, nil
}

// GetLastAppliedDeployment returns the most recently applied deployment
func (db *DB) GetLastAppliedDeployment() (*zdd.DBDeploymentRecord, error) {
	query := `
		SELECT id, name, applied_at, COALESCE(checksum, '') as checksum 
		FROM zdd_deployments.applied_deployments 
		ORDER BY applied_at DESC 
		LIMIT 1
	`

	var d zdd.DBDeploymentRecord
	err := db.pool.QueryRow(db.ctx, query).Scan(&d.ID, &d.Name, &d.AppliedAt, &d.Checksum)
	if err != nil {
		if err == pgx.ErrNoRows {
			return nil, nil // No deployments applied yet
		}
		return nil, fmt.Errorf("failed to get last applied deployment: %w", err)
	}

	return &d, nil
}

// RecordDeployment records that a deployment has been applied
func (db *DB) RecordDeployment(deployment zdd.Deployment, checksum string) error {
	query := `
		INSERT INTO zdd_deployments.applied_deployments (id, name, applied_at, checksum)
		VALUES ($1, $2, NOW(), $3)
	`

	_, err := db.pool.Exec(db.ctx, query, deployment.ID, deployment.Name, checksum)
	if err != nil {
		return fmt.Errorf("failed to record deployment %s: %w", deployment.ID, err)
	}

	return nil
}

// ExecuteSQLInTransaction executes SQL statements within a transaction
func (db *DB) ExecuteSQLInTransaction(sqlStatements ...string) error {
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
