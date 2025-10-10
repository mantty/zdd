package zdd_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	pgTest "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mantty/zdd"
	"github.com/mantty/zdd/postgres"
)

// setupTestDB creates a test database connection and cleans it up
func setupTestDB(t *testing.T) (*postgres.DB, string) {
	t.Helper()

	ctx := context.Background()
	pgContainer, err := pgTest.Run(context.Background(),
		"postgres:17-alpine",
		pgTest.WithDatabase("test"),
		pgTest.WithUsername("user"),
		pgTest.WithPassword("password"),
		pgTest.BasicWaitStrategies(),
	)

	t.Cleanup(func() {
		testcontainers.CleanupContainer(t, pgContainer)
	})

	if err != nil {
		t.Fatalf("Failed to create database: %v", err)
	}

	dbURL, err := pgContainer.ConnectionString(ctx)
	if err != nil {
		t.Fatalf("Failed to get test database connection string: %v", err)
	}

	// TODO: There shouldn't be any need to establish the DB connection
	// within the testing code - given the app itself will only take a string
	db, err := postgres.NewDB(ctx, dbURL)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Ensure migration schema exists (should be auto-created by NewDB)
	if err := db.ExecuteSQLInTransaction([]string{"SELECT COUNT(*) FROM zdd_migrations.applied_migrations"}); err != nil {
		t.Fatalf("Migration schema should exist after NewDB initialization: %v", err)
	}

	return db, dbURL
}

// cleanupTestDB removes test data from the database
// TODO: Completely reset DB schemas between test cases (below does not do that)
// func cleanupTestDB(db *postgres.DB) error {
// 	// Drop any test tables and the zdd_migrations schema
// 	cleanupSQL := []string{
// 		"DROP TABLE IF EXISTS test_users CASCADE",
// 		"DROP TABLE IF EXISTS test_posts CASCADE",
// 		"DROP TABLE IF EXISTS categories CASCADE",
// 		"DROP SCHEMA IF EXISTS zdd_migrations CASCADE",
// 	}

// 	for _, sql := range cleanupSQL {
// 		if err := db.ExecuteSQLInTransaction([]string{sql}); err != nil {
// 			// Ignore errors during cleanup
// 			continue
// 		}
// 	}

// 	return nil
// }

// createTestMigrationDir creates a temporary migration directory for testing
func createTestMigrationDir(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()
	migrationsDir := filepath.Join(tempDir, "migrations")

	if err := os.MkdirAll(migrationsDir, 0755); err != nil {
		t.Fatalf("Failed to create migrations directory: %v", err)
	}

	return migrationsDir
}

// getMigrationFilePath returns the path to a specific SQL file for a migration
func getMigrationFilePath(m *zdd.Migration, fileName string) string {
	return filepath.Join(m.Directory, fileName)
}

func TestMigrationManager_CreateMigration(t *testing.T) {
	migrationsDir := createTestMigrationDir(t)

	migration, err := zdd.CreateMigration(migrationsDir, "test_migration")
	if err != nil {
		t.Fatalf("Failed to create migration: %v", err)
	}

	// Verify migration properties
	if migration.ID == "" {
		t.Error("Migration ID should not be empty")
	}
	if migration.Name != "test_migration" {
		t.Errorf("Expected migration name 'test_migration', got '%s'", migration.Name)
	}

	// Load the migration to verify files were created
	migrations, err := zdd.LoadMigrations(migrationsDir)
	if err != nil {
		t.Fatalf("Failed to load migrations: %v", err)
	}
	if len(migrations) != 1 {
		t.Fatalf("Expected 1 migration, got %d", len(migrations))
	}

	loadedMigration := migrations[0]
	if len(loadedMigration.ExpandSQLFiles) != 1 {
		t.Errorf("Expected 1 expand SQL file, got %d", len(loadedMigration.ExpandSQLFiles))
	}
	if len(loadedMigration.MigrateSQLFiles) != 1 {
		t.Errorf("Expected 1 migrate SQL file, got %d", len(loadedMigration.MigrateSQLFiles))
	}
	if len(loadedMigration.ContractSQLFiles) != 1 {
		t.Errorf("Expected 1 contract SQL file, got %d", len(loadedMigration.ContractSQLFiles))
	}

	// Verify files were created
	if _, err := os.Stat(loadedMigration.ExpandSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Expand SQL file should exist")
	}
	if _, err := os.Stat(loadedMigration.MigrateSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Migrate SQL file should exist")
	}
	if _, err := os.Stat(loadedMigration.ContractSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Contract SQL file should exist")
	}
}

func TestMigrationManager_LoadMigrations(t *testing.T) {
	migrationsDir := createTestMigrationDir(t)

	// Create first migration
	migration1, err := zdd.CreateMigration(migrationsDir, "first_migration")
	if err != nil {
		t.Fatalf("Failed to create first migration: %v", err)
	}

	// Create second migration
	migration2, err := zdd.CreateMigration(migrationsDir, "second_migration")
	if err != nil {
		t.Fatalf("Failed to create second migration: %v", err)
	}

	// Load migrations to get file paths
	migrations, err := zdd.LoadMigrations(migrationsDir)
	if err != nil {
		t.Fatalf("Failed to load migrations: %v", err)
	}

	// Add some SQL content to the first migration's expand.sql
	testSQL := "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(255));"
	if err := os.WriteFile(migrations[0].ExpandSQLFiles[0].Path, []byte(testSQL), 0644); err != nil {
		t.Fatalf("Failed to write test SQL: %v", err)
	}

	// Load migrations again to verify content
	migrations, err = zdd.LoadMigrations(migrationsDir)
	if err != nil {
		t.Fatalf("Failed to load migrations: %v", err)
	}

	// Verify migrations are loaded and sorted
	if len(migrations) != 2 {
		t.Errorf("Expected 2 migrations, got %d", len(migrations))
	}

	if migrations[0].ID != migration1.ID {
		t.Errorf("Expected first migration ID %s, got %s", migration1.ID, migrations[0].ID)
	}
	if migrations[1].ID != migration2.ID {
		t.Errorf("Expected second migration ID %s, got %s", migration2.ID, migrations[1].ID)
	}

	// Verify SQL content was loaded
	if len(migrations[0].ExpandSQLFiles) == 0 || migrations[0].ExpandSQLFiles[0].Content != testSQL {
		t.Error("Pre SQL content should be loaded correctly")
	}
}

func TestDatabaseProvider_InitAndQuery(t *testing.T) {
	db, _ := setupTestDB(t)

	// Test getting applied migrations (should be empty initially)
	applied, err := db.GetAppliedMigrations()
	if err != nil {
		t.Fatalf("Failed to get applied migrations: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("Expected 0 applied migrations, got %d", len(applied))
	}

	// Test getting last applied migration (should be nil)
	last, err := db.GetLastAppliedMigration()
	if err != nil {
		t.Fatalf("Failed to get last applied migration: %v", err)
	}
	if last != nil {
		t.Error("Expected no last applied migration, got one")
	}
}

func TestMigrationRunner_ApplySimpleMigration(t *testing.T) {
	db, _ := setupTestDB(t)
	migrationsDir := createTestMigrationDir(t)

	// Create a migration with SQL that creates a table
	migration, err := zdd.CreateMigration(migrationsDir, "create_users_table")
	if err != nil {
		t.Fatalf("Failed to create migration: %v", err)
	}

	// Add SQL content
	createTableSQL := `
CREATE TABLE test_users (
    id SERIAL PRIMARY KEY,
    name VARCHAR(255) NOT NULL,
    email VARCHAR(255) UNIQUE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW()
);
`
	if err := os.WriteFile(getMigrationFilePath(migration, "expand.sql"), []byte(createTableSQL), 0644); err != nil {
		t.Fatalf("Failed to write SQL: %v", err)
	}

	// Create runner
	executor := zdd.NewShellCommandExecutor(0)
	runner := zdd.NewMigrationRunner(db, migrationsDir, executor)

	// Run migrations
	ctx := context.Background()
	if err := runner.RunMigrations(ctx); err != nil {
		t.Fatalf("Failed to run migrations: %v", err)
	}

	// Verify migration was recorded
	applied, err := db.GetAppliedMigrations()
	if err != nil {
		t.Fatalf("Failed to get applied migrations: %v", err)
	}
	if len(applied) != 1 {
		t.Errorf("Expected 1 applied migration, got %d", len(applied))
	}
	if applied[0].ID != migration.ID {
		t.Errorf("Expected migration ID %s, got %s", migration.ID, applied[0].ID)
	}

	// Verify table was created by trying to query it
	if err := db.ExecuteSQLInTransaction([]string{"SELECT COUNT(*) FROM test_users"}); err != nil {
		t.Errorf("Table should have been created: %v", err)
	}
}

func TestMigrationRunner_ExpandContractPattern(t *testing.T) {
	db, _ := setupTestDB(t)
	migrationsDir := createTestMigrationDir(t)

	// First, create a base table migration and apply it
	baseMigration, err := zdd.CreateMigration(migrationsDir, "create_base_table")
	if err != nil {
		t.Fatalf("Failed to create base migration: %v", err)
	}

	baseSQL := `CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL);`
	if err := os.WriteFile(getMigrationFilePath(baseMigration, "expand.sql"), []byte(baseSQL), 0644); err != nil {
		t.Fatalf("Failed to write base SQL: %v", err)
	}

	executor := zdd.NewShellCommandExecutor(0)
	runner := zdd.NewMigrationRunner(db, migrationsDir, executor)
	ctx := context.Background()

	if err := runner.RunMigrations(ctx); err != nil {
		t.Fatalf("Failed to run base migration: %v", err)
	}

	// Create an expand-contract migration and apply it separately
	expandContractMigration, err := zdd.CreateMigration(migrationsDir, "add_email_column")
	if err != nil {
		t.Fatalf("Failed to create expand-contract migration: %v", err)
	}

	// Pre-migration: Add column as nullable
	preSQL := `ALTER TABLE test_users ADD COLUMN email VARCHAR(255);`
	if err := os.WriteFile(getMigrationFilePath(expandContractMigration, "expand.sql"), []byte(preSQL), 0644); err != nil {
		t.Fatalf("Failed to write pre SQL: %v", err)
	}

	// Post-migration: Make column required
	postSQL := `ALTER TABLE test_users ALTER COLUMN email SET NOT NULL;`
	if err := os.WriteFile(getMigrationFilePath(expandContractMigration, "contract.sql"), []byte(postSQL), 0644); err != nil {
		t.Fatalf("Failed to write post SQL: %v", err)
	}

	if err := runner.RunMigrations(ctx); err != nil {
		t.Fatalf("Failed to run expand/contract migration: %v", err)
	}

	// Verify both migrations were applied
	applied, err := db.GetAppliedMigrations()
	if err != nil {
		t.Fatalf("Failed to get applied migrations: %v", err)
	}
	if len(applied) != 2 {
		t.Errorf("Expected 2 applied migrations, got %d", len(applied))
	}

	// Verify the table structure is correct (email column should be NOT NULL)
	// We can test this by trying to insert a row without email - it should fail
	err = db.ExecuteSQLInTransaction([]string{"INSERT INTO test_users (name) VALUES ('test')"})
	if err == nil {
		t.Error("Expected error when inserting without email (column should be NOT NULL)")
	}

	// But inserting with email should work
	err = db.ExecuteSQLInTransaction([]string{"INSERT INTO test_users (name, email) VALUES ('test', 'test@example.com')"})
	if err != nil {
		t.Errorf("Should be able to insert with email: %v", err)
	}
}

// TestMigrationBundles is a table-driven test that discovers and runs migration test bundles
func TestMigrationBundles(t *testing.T) {
	testdataDir := "testdata"

	// Discover all test bundles
	entries, err := os.ReadDir(testdataDir)
	if err != nil {
		t.Fatalf("Failed to read testdata directory: %v", err)
	}

	var testCases []string
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		// Check if this directory has an expected_schema.sql file
		bundlePath := filepath.Join(testdataDir, entry.Name())
		expectedSchemaPath := filepath.Join(bundlePath, "expected_schema.sql")

		if _, err := os.Stat(expectedSchemaPath); err == nil {
			testCases = append(testCases, bundlePath)
		}
	}

	if len(testCases) == 0 {
		t.Skip("No test bundles found (directories with expected_schema.sql)")
	}

	for _, bundlePath := range testCases {
		bundleName := filepath.Base(bundlePath)

		t.Run(bundleName, func(t *testing.T) {
			runMigrationBundleTest(t, bundlePath)
		})
	}
}

// runMigrationBundleTest executes a single migration bundle test
func runMigrationBundleTest(t *testing.T, bundlePath string) {
	// Setup test database
	db, _ := setupTestDB(t)
	defer db.Close()

	// Get absolute path
	absBundlePath, _ := filepath.Abs(bundlePath)

	// Create runner
	executor := zdd.NewShellCommandExecutor(0)
	runner := zdd.NewMigrationRunner(db, absBundlePath, executor)

	// Run migrations
	ctx := context.Background()
	err := runner.RunMigrations(ctx)

	if err != nil {
		t.Fatalf("Migration failed: %v", err)
	}

	// Validate full schema against expected_schema.sql
	expectedSchemaPath := filepath.Join(bundlePath, "expected_schema.sql")
	expectedSchemaBytes, err := os.ReadFile(expectedSchemaPath)

	actualSchema, err2 := db.DumpSchema()
	if err2 != nil {
		t.Fatalf("Failed to dump schema: %v", err2)
	}
	actualSchema = strings.TrimSpace(actualSchema)

	if err != nil {
		// Expected schema file doesn't exist - print actual schema to help create it
		t.Fatalf("Failed to read expected schema file: %v\n\nActual schema (copy this to %s):\n%s",
			err, expectedSchemaPath, actualSchema)
	}

	expectedSchema := strings.TrimSpace(string(expectedSchemaBytes))

	if actualSchema != expectedSchema {
		t.Errorf("Schema mismatch!\n\nExpected:\n%s\n\nActual:\n%s\n\nDiff:\n%s",
			expectedSchema, actualSchema, generateSchemaDiff(expectedSchema, actualSchema))
	}
}

// generateSchemaDiff creates a simple line-by-line diff of two schemas
func generateSchemaDiff(expected, actual string) string {
	expectedLines := strings.Split(expected, "\n")
	actualLines := strings.Split(actual, "\n")

	var diff strings.Builder
	maxLines := len(expectedLines)
	if len(actualLines) > maxLines {
		maxLines = len(actualLines)
	}

	for i := 0; i < maxLines; i++ {
		var expLine, actLine string
		if i < len(expectedLines) {
			expLine = expectedLines[i]
		}
		if i < len(actualLines) {
			actLine = actualLines[i]
		}

		if expLine != actLine {
			if expLine != "" {
				diff.WriteString(fmt.Sprintf("- %s\n", expLine))
			}
			if actLine != "" {
				diff.WriteString(fmt.Sprintf("+ %s\n", actLine))
			}
		}
	}

	if diff.Len() == 0 {
		return "(no differences found - likely whitespace)"
	}
	return diff.String()
}
