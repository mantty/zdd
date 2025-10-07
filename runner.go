package zdd

import (
	"context"
	"fmt"
	"strings"
)

// MigrationRunner handles the core migration execution logic
type MigrationRunner struct {
	db             DatabaseProvider
	migrationsPath string
	executor       CommandExecutor
	config         *Config
}

// NewMigrationRunner creates a new migration runner
func NewMigrationRunner(db DatabaseProvider, migrationsPath string, executor CommandExecutor, config *Config) *MigrationRunner {
	return &MigrationRunner{
		db:             db,
		migrationsPath: migrationsPath,
		executor:       executor,
		config:         config,
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
	for _, migration := range status.Pending {
		if err := mr.applyMigrationWithPhases(ctx, migration); err != nil {
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
func (mr *MigrationRunner) applyMigrationWithPhases(ctx context.Context, migration Migration) error {
	fmt.Printf("Applying migration %s: %s\n", migration.ID, migration.Name)

	// Phase 1: Expand - Prepare database for new schema
	if err := mr.applyPhase(migration, "expand", migration.ExpandSQLFiles); err != nil {
		return fmt.Errorf("expand phase failed: %w", err)
	}

	// Phase 2: Migrate - Core schema changes
	if err := mr.applyPhase(migration, "migrate", migration.MigrateSQLFiles); err != nil {
		return fmt.Errorf("migrate phase failed: %w", err)
	}

	// Phase 3: Contract - Remove old schema elements
	if err := mr.applyPhase(migration, "contract", migration.ContractSQLFiles); err != nil {
		return fmt.Errorf("contract phase failed: %w", err)
	}

	// Phase 4: Post - Validation and testing
	if err := mr.applyPostPhase(migration); err != nil {
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
func (mr *MigrationRunner) applyPhase(migration Migration, phaseName string, sqlFiles []SQLFile) error {
	// Apply SQL files if they exist
	if err := mr.applySQLFiles(sqlFiles, phaseName); err != nil {
		return fmt.Errorf("SQL execution failed: %w", err)
	}

	// Execute command for this phase
	if migration.Config != nil {
		var command *string
		switch phaseName {
		case "expand":
			command = migration.Config.Expand
		case "migrate":
			command = migration.Config.Migrate
		case "contract":
			command = migration.Config.Contract
		}

		if command != nil && *command != "" {
			fmt.Printf("Executing %s phase command...\n", phaseName)
			if err := mr.executor.ExecuteCommand(*command, migration.Directory); err != nil {
				return fmt.Errorf("command execution failed: %w", err)
			}
		}
	}

	return nil
}

// applyPostPhase applies the post-validation phase
func (mr *MigrationRunner) applyPostPhase(migration Migration) error {
	if migration.Config != nil && migration.Config.Post != nil && *migration.Config.Post != "" {
		fmt.Println("Executing post-validation command...")
		if err := mr.executor.ExecuteCommand(*migration.Config.Post, migration.Directory); err != nil {
			return fmt.Errorf("post-validation failed: %w", err)
		}
	}
	return nil
}

// applySQLFiles applies a sequence of SQL files in order
func (mr *MigrationRunner) applySQLFiles(sqlFiles []SQLFile, phase string) error {
	for _, sqlFile := range sqlFiles {
		content := strings.TrimSpace(sqlFile.Content)
		if content == "" ||
			strings.HasPrefix(content, "-- Expand phase SQL (optional)") ||
			strings.HasPrefix(content, "-- Migrate phase SQL (optional)") ||
			strings.HasPrefix(content, "-- Contract phase SQL (optional)") {
			continue // Skip empty or template files
		}

		fmt.Printf("  Executing %s SQL file: %s\n", phase, sqlFile.Path)

		// Split content by semicolons for individual statements
		statements := mr.splitSQLStatements(content)
		if err := mr.db.ExecuteSQLInTransaction(statements); err != nil {
			return fmt.Errorf("failed to execute %s SQL file %s: %w", phase, sqlFile.Path, err)
		}
	}
	return nil
}

// splitSQLStatements splits SQL content into individual statements
func (mr *MigrationRunner) splitSQLStatements(content string) []string {
	// Simple splitting by semicolon - this could be enhanced for more complex SQL
	statements := strings.Split(content, ";")
	var cleaned []string

	for _, stmt := range statements {
		stmt = strings.TrimSpace(stmt)
		if stmt != "" && !strings.HasPrefix(stmt, "--") {
			cleaned = append(cleaned, stmt)
		}
	}

	return cleaned
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
