package postgres

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	pgTest "github.com/testcontainers/testcontainers-go/modules/postgres"
)

func TestNewDBInitializesMigrationSchema(t *testing.T) {
	ctx := context.Background()
	container, err := pgTest.Run(ctx,
		"postgres:17-alpine",
		pgTest.WithDatabase("test"),
		pgTest.WithUsername("user"),
		pgTest.WithPassword("password"),
		pgTest.BasicWaitStrategies(),
	)
	if err != nil {
		t.Fatalf("failed to start postgres container: %v", err)
	}
	t.Cleanup(func() {
		testcontainers.CleanupContainer(t, container)
	})

	dbURL, err := container.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("failed to get connection string: %v", err)
	}

	db, err := NewDB(ctx, dbURL)
	if err != nil {
		t.Fatalf("failed to create db: %v", err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	var schemaName string
	err = db.pool.QueryRow(ctx, "SELECT schema_name FROM information_schema.schemata WHERE schema_name = 'zdd_migrations'").Scan(&schemaName)
	if err != nil {
		t.Fatalf("expected zdd_migrations schema to exist: %v", err)
	}

	if schemaName != "zdd_migrations" {
		t.Fatalf("unexpected schema name: %s", schemaName)
	}

	if err := db.ExecuteSQLInTransaction([]string{"SELECT COUNT(*) FROM zdd_migrations.applied_migrations"}); err != nil {
		t.Fatalf("expected applied_migrations table to exist: %v", err)
	}
}
