package zdd_test

import (
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/testcontainers/testcontainers-go"
	pgTest "github.com/testcontainers/testcontainers-go/modules/postgres"

	"github.com/mantty/zdd"
	"github.com/mantty/zdd/postgres"
)

var (
	sharedPgContainer testcontainers.Container
	sharedDBURL       string
)

// TestMain sets up a single Postgres container for all tests
func TestMain(m *testing.M) {
	ctx := context.Background()

	// Create container once for all tests
	pgContainer, err := pgTest.Run(ctx,
		"postgres:17-alpine",
		pgTest.WithDatabase("test"),
		pgTest.WithUsername("user"),
		pgTest.WithPassword("password"),
		pgTest.BasicWaitStrategies(),
	)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create postgres container: %v\n", err)
		os.Exit(1)
	}

	sharedPgContainer = pgContainer

	dbURL, err := pgContainer.ConnectionString(ctx)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to get connection string: %v\n", err)
		testcontainers.CleanupContainer(nil, pgContainer)
		os.Exit(1)
	}
	sharedDBURL = dbURL

	// Connect to initialize the zdd schema in test database
	db, err := postgres.NewDB(ctx, dbURL)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to connect to test database: %v\n", err)
		testcontainers.CleanupContainer(nil, pgContainer)
		os.Exit(1)
	}
	db.Close()

	// Create a template database from the initialized test database
	// This will be much faster to clone than restoring from dump
	if err := createTemplateDatabase(ctx, pgContainer); err != nil {
		fmt.Fprintf(os.Stderr, "Failed to create template database: %v\n", err)
		testcontainers.CleanupContainer(nil, pgContainer)
		os.Exit(1)
	}

	// Run tests
	code := m.Run()

	// Cleanup container after all tests
	if sharedPgContainer != nil {
		if err := sharedPgContainer.Terminate(ctx); err != nil {
			fmt.Fprintf(os.Stderr, "Failed to terminate container: %v\n", err)
		}
	}

	os.Exit(code)
}

// createTemplateDatabase creates a template database from the current test database
func createTemplateDatabase(ctx context.Context, container testcontainers.Container) error {
	// Execute all commands in a single shell invocation to minimize Docker exec overhead
	// Using psql with multiple -c flags runs each command separately (outside transaction blocks)
	exitCode, reader, err := container.Exec(ctx, []string{
		"sh", "-c",
		`psql -U user -d postgres \
			-c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'test' AND pid <> pg_backend_pid();" \
			-c "CREATE DATABASE test_template WITH TEMPLATE test;" \
			-c "UPDATE pg_database SET datistemplate = true WHERE datname = 'test_template';"`,
	})
	if err != nil {
		return fmt.Errorf("failed to create template database: %w", err)
	}
	if exitCode != 0 {
		output, _ := io.ReadAll(reader)
		return fmt.Errorf("create template database exited with code %d: %s", exitCode, string(output))
	}

	return nil
}

// restoreDatabase restores the database from template (much faster than dump/restore)
func restoreDatabase(ctx context.Context, container testcontainers.Container) error {
	// Execute all commands in a single shell invocation to minimize Docker exec overhead
	// Using psql with multiple -c flags (they run outside transaction blocks)
	exitCode, reader, err := container.Exec(ctx, []string{
		"sh", "-c",
		`psql -U user -d postgres \
			-c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE datname = 'test' AND pid <> pg_backend_pid();" \
			-c "DROP DATABASE IF EXISTS test;" \
			-c "CREATE DATABASE test WITH TEMPLATE test_template;"`,
	})
	if err != nil {
		return fmt.Errorf("failed to restore database: %w", err)
	}
	if exitCode != 0 {
		output, _ := io.ReadAll(reader)
		return fmt.Errorf("restore database exited with code %d: %s", exitCode, string(output))
	}

	return nil
}

// setupTestDB creates a test database connection using the shared container
// and restores it to pristine state after the test completes
func setupTestDB(t *testing.T) (*postgres.DB, string) {
	t.Helper()

	ctx := context.Background()

	// Restore database to pristine state BEFORE test runs
	// This ensures each test starts with a clean slate
	if err := restoreDatabase(ctx, sharedPgContainer); err != nil {
		t.Fatalf("Failed to restore database before test: %v", err)
	}

	db, err := postgres.NewDB(ctx, sharedDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	// Just close connection when done - restoration happens at START of next test
	t.Cleanup(func() {
		db.Close()
	})

	return db, sharedDBURL
}

// setupTestDBReadOnly creates a read-only test database connection
// Use this for tests that only query and don't modify the database
func setupTestDBReadOnly(t *testing.T) (*postgres.DB, string) {
	t.Helper()

	ctx := context.Background()

	db, err := postgres.NewDB(ctx, sharedDBURL)
	if err != nil {
		t.Fatalf("Failed to connect to test database: %v", err)
	}

	t.Cleanup(func() {
		db.Close()
	})

	return db, sharedDBURL
}

// createTestDeploymentDir creates a temporary deployment directory for testing
func createTestDeploymentDir(t *testing.T) string {
	t.Helper()

	tempDir := t.TempDir()
	deploymentsDir := filepath.Join(tempDir, "migrations")

	if err := os.MkdirAll(deploymentsDir, 0755); err != nil {
		t.Fatalf("Failed to create deployments directory: %v", err)
	}

	return deploymentsDir
}

// getDeploymentFilePath returns the path to a specific SQL file for a deployment
func getDeploymentFilePath(d *zdd.Deployment, fileName string) string {
	return filepath.Join(d.Directory, fileName)
}

func TestDeploymentManager_CreateDeployment(t *testing.T) {
	deploymentsDir := createTestDeploymentDir(t)

	deployment, err := zdd.CreateDeployment(deploymentsDir, "test_deployment")
	if err != nil {
		t.Fatalf("Failed to create deployment: %v", err)
	}

	// Verify deployment properties
	if deployment.ID == "" {
		t.Error("Deployment ID should not be empty")
	}
	if deployment.Name != "test_deployment" {
		t.Errorf("Expected deployment name 'test_deployment', got '%s'", deployment.Name)
	}

	// Verify files were created on disk (check filesystem directly)
	deploymentDir := filepath.Join(deploymentsDir, deployment.ID+"_"+deployment.Name)

	expandPath := filepath.Join(deploymentDir, "expand.sql")
	if _, err := os.Stat(expandPath); os.IsNotExist(err) {
		t.Error("Expand SQL file should exist on disk")
	}

	migratePath := filepath.Join(deploymentDir, "migrate.sql")
	if _, err := os.Stat(migratePath); os.IsNotExist(err) {
		t.Error("Migrate SQL file should exist on disk")
	}

	contractPath := filepath.Join(deploymentDir, "contract.sql")
	if _, err := os.Stat(contractPath); os.IsNotExist(err) {
		t.Error("Contract SQL file should exist on disk")
	}

	// Load the deployment - empty files should not be loaded
	deployments, err := zdd.LoadDeployments(deploymentsDir)
	if err != nil {
		t.Fatalf("Failed to load deployments: %v", err)
	}
	if len(deployments) != 1 {
		t.Fatalf("Expected 1 deployment, got %d", len(deployments))
	}

	loadedDeployment := deployments[0]
	// Since files are empty, they should not be loaded (should be nil)
	if loadedDeployment.ExpandSQLFile != nil {
		t.Error("Expected expand SQL file to be nil (file is empty)")
	}
	if loadedDeployment.MigrateSQLFile != nil {
		t.Error("Expected migrate SQL file to be nil (file is empty)")
	}
	if loadedDeployment.ContractSQLFile != nil {
		t.Error("Expected contract SQL file to be nil (file is empty)")
	}
}

func TestDeploymentManager_LoadDeployments(t *testing.T) {
	deploymentsDir := createTestDeploymentDir(t)

	// Create first deployment
	deployment1, err := zdd.CreateDeployment(deploymentsDir, "first_deployment")
	if err != nil {
		t.Fatalf("Failed to create first deployment: %v", err)
	}

	// Create second deployment
	deployment2, err := zdd.CreateDeployment(deploymentsDir, "second_deployment")
	if err != nil {
		t.Fatalf("Failed to create second deployment: %v", err)
	}

	// Write SQL content to the first deployment's expand.sql (using filesystem path directly)
	testSQL := "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(255));"
	deployment1Dir := filepath.Join(deploymentsDir, deployment1.ID+"_"+deployment1.Name)
	expandPath := filepath.Join(deployment1Dir, "expand.sql")
	if err := os.WriteFile(expandPath, []byte(testSQL), 0644); err != nil {
		t.Fatalf("Failed to write test SQL: %v", err)
	}

	// Load deployments to verify content
	deployments, err := zdd.LoadDeployments(deploymentsDir)
	if err != nil {
		t.Fatalf("Failed to load deployments: %v", err)
	}

	// Verify deployments are loaded and sorted
	if len(deployments) != 2 {
		t.Errorf("Expected 2 deployments, got %d", len(deployments))
	}

	if deployments[0].ID != deployment1.ID {
		t.Errorf("Expected first deployment ID %s, got %s", deployment1.ID, deployments[0].ID)
	}
	if deployments[1].ID != deployment2.ID {
		t.Errorf("Expected second deployment ID %s, got %s", deployment2.ID, deployments[1].ID)
	}

	// Verify SQL content was loaded
	if deployments[0].ExpandSQLFile == nil || deployments[0].ExpandSQLFile.Content != testSQL {
		t.Error("Expand SQL content should be loaded correctly")
	}
}

func TestDatabaseProvider_InitAndQuery(t *testing.T) {
	// This test only reads from DB, no need to restore
	db, _ := setupTestDBReadOnly(t)

	// Test getting applied deployments (should be empty initially)
	applied, err := db.GetAppliedDeployments()
	if err != nil {
		t.Fatalf("Failed to get applied deployments: %v", err)
	}
	if len(applied) != 0 {
		t.Errorf("Expected 0 applied deployments, got %d", len(applied))
	}

	// Test getting last applied deployment (should be nil)
	last, err := db.GetLastAppliedDeployment()
	if err != nil {
		t.Fatalf("Failed to get last applied deployment: %v", err)
	}
	if last != nil {
		t.Error("Expected no last applied deployment, got one")
	}
}

func TestDeploymentRunner_ApplySimpleDeployment(t *testing.T) {
	db, _ := setupTestDB(t)
	deploymentsDir := createTestDeploymentDir(t)

	// Create a deployment with SQL that creates a table
	deployment, err := zdd.CreateDeployment(deploymentsDir, "create_users_table")
	if err != nil {
		t.Fatalf("Failed to create deployment: %v", err)
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
	if err := os.WriteFile(getDeploymentFilePath(deployment, "expand.sql"), []byte(createTableSQL), 0644); err != nil {
		t.Fatalf("Failed to write SQL: %v", err)
	}

	// Build and execute plan
	plan, err := zdd.BuildPlan(deploymentsDir, db)
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	if err := plan.Execute(); err != nil {
		t.Fatalf("Failed to execute plan: %v", err)
	}

	// Verify deployment was recorded
	applied, err := db.GetAppliedDeployments()
	if err != nil {
		t.Fatalf("Failed to get applied deployments: %v", err)
	}
	if len(applied) != 1 {
		t.Errorf("Expected 1 applied deployment, got %d", len(applied))
	}
	if applied[0].ID != deployment.ID {
		t.Errorf("Expected deployment ID %s, got %s", deployment.ID, applied[0].ID)
	}

	// Verify table was created by trying to query it
	if err := db.ExecuteSQLInTransaction("SELECT COUNT(*) FROM test_users"); err != nil {
		t.Errorf("Table should have been created: %v", err)
	}
}

func TestDeploymentRunner_ExpandContractPattern(t *testing.T) {
	db, _ := setupTestDB(t)
	deploymentsDir := createTestDeploymentDir(t)

	// First, create a base table deployment and apply it
	baseDeployment, err := zdd.CreateDeployment(deploymentsDir, "create_base_table")
	if err != nil {
		t.Fatalf("Failed to create base deployment: %v", err)
	}

	baseSQL := `CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(255) NOT NULL);`
	if err := os.WriteFile(getDeploymentFilePath(baseDeployment, "expand.sql"), []byte(baseSQL), 0644); err != nil {
		t.Fatalf("Failed to write base SQL: %v", err)
	}

	plan, err := zdd.BuildPlan(deploymentsDir, db)
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	if err := plan.Execute(); err != nil {
		t.Fatalf("Failed to execute plan: %v", err)
	}

	// Create an expand-contract deployment and apply it separately
	expandContractDeployment, err := zdd.CreateDeployment(deploymentsDir, "add_email_column")
	if err != nil {
		t.Fatalf("Failed to create expand-contract deployment: %v", err)
	}

	// Pre-deployment: Add column as nullable
	preSQL := `ALTER TABLE test_users ADD COLUMN email VARCHAR(255);`
	if err := os.WriteFile(getDeploymentFilePath(expandContractDeployment, "expand.sql"), []byte(preSQL), 0644); err != nil {
		t.Fatalf("Failed to write pre SQL: %v", err)
	}

	// Post-deployment: Make column required
	postSQL := `ALTER TABLE test_users ALTER COLUMN email SET NOT NULL;`
	if err := os.WriteFile(getDeploymentFilePath(expandContractDeployment, "contract.sql"), []byte(postSQL), 0644); err != nil {
		t.Fatalf("Failed to write post SQL: %v", err)
	}

	plan2, err := zdd.BuildPlan(deploymentsDir, db)
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	if err := plan2.Execute(); err != nil {
		t.Fatalf("Failed to execute plan: %v", err)
	}

	// Verify both deployments were applied
	applied, err := db.GetAppliedDeployments()
	if err != nil {
		t.Fatalf("Failed to get applied deployments: %v", err)
	}
	if len(applied) != 2 {
		t.Errorf("Expected 2 applied deployments, got %d", len(applied))
	}

	// Verify the table structure is correct (email column should be NOT NULL)
	// We can test this by trying to insert a row without email - it should fail
	err = db.ExecuteSQLInTransaction("INSERT INTO test_users (name) VALUES ('test')")
	if err == nil {
		t.Error("Expected error when inserting without email (column should be NOT NULL)")
	}

	// But inserting with email should work
	err = db.ExecuteSQLInTransaction("INSERT INTO test_users (name, email) VALUES ('test', 'test@example.com')")
	if err != nil {
		t.Errorf("Should be able to insert with email: %v", err)
	}
}

// TestDeploymentBundles is a table-driven test that discovers and runs deployment test bundles
func TestDeploymentBundles(t *testing.T) {
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
			runDeploymentBundleTest(t, bundlePath)
		})
	}
}

// runDeploymentBundleTest executes a single deployment bundle test
func runDeploymentBundleTest(t *testing.T, bundlePath string) {
	// Setup test database
	db, _ := setupTestDB(t)

	// Get absolute path
	absBundlePath, _ := filepath.Abs(bundlePath)

	// Build and execute plan
	plan, err := zdd.BuildPlan(absBundlePath, db)
	if err != nil {
		t.Fatalf("Failed to build plan: %v", err)
	}

	err = plan.Execute()

	if err != nil {
		t.Fatalf("Deployment failed: %v", err)
	}

	// Validate full schema against expected_schema.sql
	expectedSchemaPath := filepath.Join(bundlePath, "expected_schema.sql")
	expectedSchemaBytes, err := os.ReadFile(expectedSchemaPath)

	actualSchema, err2 := dumpSchemaForTesting(db)
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

// dumpSchemaForTesting exports the current database schema for test validation
// This creates its own connection to avoid polluting production code
func dumpSchemaForTesting(db zdd.DatabaseProvider) (string, error) {
	// Get connection string from the database provider
	connStr := db.ConnectionString()

	// Create our own connection for testing schema
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		return "", fmt.Errorf("failed to parse connection string: %w", err)
	}

	pool, err := pgxpool.New(context.Background(), config.ConnString())
	if err != nil {
		return "", fmt.Errorf("failed to create test connection: %w", err)
	}
	defer pool.Close()

	var schemaDump strings.Builder
	schemaDump.WriteString("-- Schema dump generated by zdd\n\n")

	// Get table definitions
	tableQuery := `
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

	rows, err := pool.Query(context.Background(), tableQuery)
	if err != nil {
		return "", fmt.Errorf("failed to dump schema: %w", err)
	}
	defer rows.Close()

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

	// Get index definitions
	indexQuery := `
		SELECT 
			schemaname,
			indexname,
			indexdef
		FROM pg_indexes
		WHERE schemaname NOT IN ('information_schema', 'pg_catalog', 'pg_toast')
		  AND indexname NOT LIKE '%_pkey'
		ORDER BY schemaname, indexname
	`

	indexRows, err := pool.Query(context.Background(), indexQuery)
	if err != nil {
		return "", fmt.Errorf("failed to dump indexes: %w", err)
	}
	defer indexRows.Close()

	for indexRows.Next() {
		var schema, indexName, indexDef string
		if err := indexRows.Scan(&schema, &indexName, &indexDef); err != nil {
			return "", fmt.Errorf("failed to scan index definition: %w", err)
		}

		schemaDump.WriteString(fmt.Sprintf("-- Index: %s\n", indexName))
		schemaDump.WriteString(indexDef)
		schemaDump.WriteString(";\n\n")
	}

	if err := indexRows.Err(); err != nil {
		return "", fmt.Errorf("error iterating index dump: %w", err)
	}

	return schemaDump.String(), nil
}
