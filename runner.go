package zdd

import (
	"context"
	"fmt"
)

type (
	// MigrationRunner handles the core migration execution logic
	MigrationRunner struct {
		db             DatabaseProvider
		migrationsPath string
		executor       CommandExecutor
	}
)

// NewMigrationRunner creates a new migration runner
func NewMigrationRunner(db DatabaseProvider, migrationsPath string, executor CommandExecutor) *MigrationRunner {
	return &MigrationRunner{
		db:             db,
		migrationsPath: migrationsPath,
		executor:       executor,
	}
}

// RunMigrations executes the full migration process
func (mr *MigrationRunner) RunMigrations(ctx context.Context) error {
	// 1. Load local migrations
	localMigrations, err := LoadMigrations(mr.migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to load local migrations: %w", err)
	}

	// 2. Get applied migrations from DB
	appliedMigrations, err := mr.db.GetAppliedMigrations()
	if err != nil {
		return fmt.Errorf("failed to get applied migrations: %w", err)
	}

	// 3. Compare and get migration status
	status := CompareMigrations(localMigrations, appliedMigrations)

	// 4. Validate outstanding migrations
	if err := ValidateOutstandingMigrations(status.Pending); err != nil {
		return fmt.Errorf("migration validation failed: %w", err)
	}

	if len(status.Pending) == 0 {
		fmt.Println("No pending migrations to apply")
		return nil
	}

	// 5. Dump current schema before migrations
	schemaBefore, err := mr.db.DumpSchema()
	if err != nil {
		return fmt.Errorf("failed to dump schema before migrations: %w", err)
	}

	// 6. Apply all pending migrations using expand-migrate-contract workflow
	for i, migration := range status.Pending {
		isHead := i == len(status.Pending)-1 // Last migration is the head
		if err := mr.applyMigrationWithPhases(ctx, migration, isHead); err != nil {
			return fmt.Errorf("failed to apply migration %s: %w", migration.ID, err)
		}
	}

	// 7. Dump schema after migrations and generate diff
	schemaAfter, err := mr.db.DumpSchema()
	if err != nil {
		return fmt.Errorf("failed to dump schema after migrations: %w", err)
	}

	if err := mr.generateSchemaDiff(schemaBefore, schemaAfter); err != nil {
		return fmt.Errorf("failed to generate schema diff: %w", err)
	}

	fmt.Println("All migrations applied successfully!")
	return nil
}

// applyMigrationWithPhases applies a migration using the expand-migrate-contract workflow
func (mr *MigrationRunner) applyMigrationWithPhases(ctx context.Context, migration Migration, isHead bool) error {
	fmt.Printf("Applying migration %s: %s\n", migration.ID, migration.Name)

	// Phase 1: Expand - Prepare database for new schema
	if err := mr.applyPhase(migration, "expand", migration.ExpandScript, migration.ExpandSQLFiles, isHead); err != nil {
		return fmt.Errorf("expand phase failed: %w", err)
	}

	// Phase 2: Migrate - Core schema changes
	if err := mr.applyPhase(migration, "migrate", migration.MigrateScript, migration.MigrateSQLFiles, isHead); err != nil {
		return fmt.Errorf("migrate phase failed: %w", err)
	}

	// Phase 3: Contract - Remove old schema elements
	if err := mr.applyPhase(migration, "contract", migration.ContractScript, migration.ContractSQLFiles, isHead); err != nil {
		return fmt.Errorf("contract phase failed: %w", err)
	}

	// Phase 4: Post - Validation and testing
	if err := mr.applyPostPhase(migration, isHead); err != nil {
		return fmt.Errorf("post phase failed: %w", err)
	}

	// Record migration as applied
	checksum := CalculateChecksum(migration)
	if err := mr.db.RecordMigration(migration, checksum); err != nil {
		return fmt.Errorf("failed to record migration: %w", err)
	}

	fmt.Printf("Migration %s applied successfully\n", migration.ID)
	return nil
}

// applyPhase applies a specific phase (expand, migrate, or contract)
func (mr *MigrationRunner) applyPhase(migration Migration, phaseName string, script *ScriptFile, sqlFiles []SQLFile, isHead bool) error {
	// Execute shell script before SQL (if exists)
	if script != nil {
		if err := mr.executeScript(script, migration, phaseName, isHead); err != nil {
			return fmt.Errorf("script execution failed: %w", err)
		}
	}

	// Apply SQL files if they exist
	if err := mr.applySQLFiles(sqlFiles, phaseName); err != nil {
		return fmt.Errorf("SQL execution failed: %w", err)
	}

	return nil
}

// applyPostPhase applies the post-validation phase
func (mr *MigrationRunner) applyPostPhase(migration Migration, isHead bool) error {
	if migration.PostScript != nil {
		if err := mr.executeScript(migration.PostScript, migration, "post", isHead); err != nil {
			return fmt.Errorf("post-validation failed: %w", err)
		}
	}
	return nil
}

// executeScript executes a shell script with ZDD environment variables
func (mr *MigrationRunner) executeScript(script *ScriptFile, migration Migration, phase string, isHead bool) error {
	// Set environment variables
	env := map[string]string{
		"ZDD_IS_HEAD":         fmt.Sprintf("%t", isHead),
		"ZDD_MIGRATION_ID":    migration.ID,
		"ZDD_MIGRATION_NAME":  migration.Name,
		"ZDD_PHASE":           phase,
		"ZDD_MIGRATIONS_PATH": mr.migrationsPath,
	}

	scriptType := "migration-specific"
	if script.IsDefault {
		scriptType = "default"
	}

	fmt.Printf("  Executing %s %s script: %s\n", scriptType, phase, script.Path)

	if err := mr.executor.ExecuteCommandWithEnv(script.Path, migration.Directory, env); err != nil {
		return fmt.Errorf("failed to execute script: %w", err)
	}

	return nil
}

// applySQLFiles applies a sequence of SQL files in order
func (mr *MigrationRunner) applySQLFiles(sqlFiles []SQLFile, phase string) error {
	// Skip if there's no actual SQL content (just comments/whitespace)
	if !HasNonEmptySQL(sqlFiles) {
		return nil
	}

	for _, sqlFile := range sqlFiles {
		// Check if this individual file has actual SQL (not just comments)
		if !HasNonEmptySQL([]SQLFile{sqlFile}) {
			continue // Skip files with only comments/whitespace
		}

		fmt.Printf("  Executing %s SQL file: %s\n", phase, sqlFile.Path)

		// Execute the entire SQL file content as-is
		// PostgreSQL can handle multiple statements and comments natively
		if err := mr.db.ExecuteSQLInTransaction([]string{sqlFile.Content}); err != nil {
			return fmt.Errorf("failed to execute %s SQL file %s: %w", phase, sqlFile.Path, err)
		}
	}
	return nil
}

// generateSchemaDiff generates and saves a diff of the schema changes
func (mr *MigrationRunner) generateSchemaDiff(before, after string) error {
	// For now, just print the diff - could be enhanced to use external diff tools
	fmt.Println("\n=== Schema Changes ===")
	if before == after {
		fmt.Println("No schema changes detected")
	} else {
		fmt.Println("Schema changes detected (detailed diff not implemented yet)")
		fmt.Printf("Before: %d characters\n", len(before))
		fmt.Printf("After: %d characters\n", len(after))
	}

	return nil
}
