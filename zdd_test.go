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

	// Ensure deployment schema exists (should be auto-created by NewDB)
	if err := db.ExecuteSQLInTransaction("SELECT COUNT(*) FROM zdd_deployments.applied_deployments"); err != nil {
		t.Fatalf("Deployment schema should exist after NewDB initialization: %v", err)
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

	// Load the deployment to verify files were created
	deployments, err := zdd.LoadDeployments(deploymentsDir)
	if err != nil {
		t.Fatalf("Failed to load deployments: %v", err)
	}
	if len(deployments) != 1 {
		t.Fatalf("Expected 1 deployment, got %d", len(deployments))
	}

	loadedDeployment := deployments[0]
	if len(loadedDeployment.ExpandSQLFiles) != 1 {
		t.Errorf("Expected 1 expand SQL file, got %d", len(loadedDeployment.ExpandSQLFiles))
	}
	if len(loadedDeployment.MigrateSQLFiles) != 1 {
		t.Errorf("Expected 1 migrate SQL file, got %d", len(loadedDeployment.MigrateSQLFiles))
	}
	if len(loadedDeployment.ContractSQLFiles) != 1 {
		t.Errorf("Expected 1 contract SQL file, got %d", len(loadedDeployment.ContractSQLFiles))
	}

	// Verify files were created
	if _, err := os.Stat(loadedDeployment.ExpandSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Expand SQL file should exist")
	}
	if _, err := os.Stat(loadedDeployment.MigrateSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Migrate SQL file should exist")
	}
	if _, err := os.Stat(loadedDeployment.ContractSQLFiles[0].Path); os.IsNotExist(err) {
		t.Error("Contract SQL file should exist")
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

	// Load deployments to get file paths
	deployments, err := zdd.LoadDeployments(deploymentsDir)
	if err != nil {
		t.Fatalf("Failed to load deployments: %v", err)
	}

	// Add some SQL content to the first deployment's expand.sql
	testSQL := "CREATE TABLE test_users (id SERIAL PRIMARY KEY, name VARCHAR(255));"
	if err := os.WriteFile(deployments[0].ExpandSQLFiles[0].Path, []byte(testSQL), 0644); err != nil {
		t.Fatalf("Failed to write test SQL: %v", err)
	}

	// Load deployments again to verify content
	deployments, err = zdd.LoadDeployments(deploymentsDir)
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
	if len(deployments[0].ExpandSQLFiles) == 0 || deployments[0].ExpandSQLFiles[0].Content != testSQL {
		t.Error("Pre SQL content should be loaded correctly")
	}
}

func TestDatabaseProvider_InitAndQuery(t *testing.T) {
	db, _ := setupTestDB(t)

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
	defer db.Close()

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
