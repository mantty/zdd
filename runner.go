package zdd

import (
	"context"
	"fmt"
)

// DeploymentRunner handles the core deployment execution logic
type DeploymentRunner struct {
	db              DatabaseProvider
	deploymentsPath string
	executor        CommandExecutor
}

// NewDeploymentRunner creates a new deployment runner
func NewDeploymentRunner(db DatabaseProvider, deploymentsPath string, executor CommandExecutor) *DeploymentRunner {
	return &DeploymentRunner{
		db:              db,
		deploymentsPath: deploymentsPath,
		executor:        executor,
	}
}

// RunDeployments executes the full deployment process
func (dr *DeploymentRunner) RunDeployments(ctx context.Context) error {
	// 1. Load local deployments
	localDeployments, err := LoadDeployments(dr.deploymentsPath)
	if err != nil {
		return fmt.Errorf("failed to load local deployments: %w", err)
	}

	// 2. Get applied deployments from DB
	appliedDeployments, err := dr.db.GetAppliedDeployments()
	if err != nil {
		return fmt.Errorf("failed to get applied deployments: %w", err)
	}

	// 3. Compare and get deployment status
	status := CompareDeployments(localDeployments, appliedDeployments)

	if len(status.Pending) == 0 {
		fmt.Println("No pending deployments to apply")
		return nil
	}

	// 4. Dump current schema before deployments
	schemaBefore, err := dr.db.DumpSchema()
	if err != nil {
		return fmt.Errorf("failed to dump schema before deployments: %w", err)
	}

	// 5. Apply all pending deployments using expand-migrate-contract workflow
	for i, deployment := range status.Pending {
		isHead := i == len(status.Pending)-1 // Last deployment is the head
		if err := dr.applyDeploymentWithPhases(ctx, deployment, isHead); err != nil {
			return fmt.Errorf("failed to apply deployment %s: %w", deployment.ID, err)
		}
	}

	// 6. Dump schema after deployments and generate diff
	schemaAfter, err := dr.db.DumpSchema()
	if err != nil {
		return fmt.Errorf("failed to dump schema after deployments: %w", err)
	}

	if err := dr.generateSchemaDiff(schemaBefore, schemaAfter); err != nil {
		return fmt.Errorf("failed to generate schema diff: %w", err)
	}

	fmt.Println("All deployments applied successfully!")
	return nil
}

// applyDeploymentWithPhases applies a deployment using the expand-migrate-contract workflow
func (dr *DeploymentRunner) applyDeploymentWithPhases(ctx context.Context, deployment Deployment, isHead bool) error {
	fmt.Printf("Applying deployment %s: %s\n", deployment.ID, deployment.Name)

	// Phase 1: Expand - Prepare database for new schema
	if err := dr.applyPhase(deployment, "expand", deployment.ExpandScript, deployment.ExpandSQLFiles, isHead); err != nil {
		return fmt.Errorf("expand phase failed: %w", err)
	}

	// Phase 2: Migrate - Core schema changes
	if err := dr.applyPhase(deployment, "migrate", deployment.MigrateScript, deployment.MigrateSQLFiles, isHead); err != nil {
		return fmt.Errorf("migrate phase failed: %w", err)
	}

	// Phase 3: Contract - Remove old schema elements
	if err := dr.applyPhase(deployment, "contract", deployment.ContractScript, deployment.ContractSQLFiles, isHead); err != nil {
		return fmt.Errorf("contract phase failed: %w", err)
	}

	// Phase 4: Post - Validation and testing
	if err := dr.applyPostPhase(deployment, isHead); err != nil {
		return fmt.Errorf("post phase failed: %w", err)
	}

	// Record deployment as applied
	checksum := CalculateChecksum(deployment)
	if err := dr.db.RecordDeployment(deployment, checksum); err != nil {
		return fmt.Errorf("failed to record deployment: %w", err)
	}

	fmt.Printf("Deployment %s applied successfully\n", deployment.ID)
	return nil
}

// applyPhase applies a specific phase (expand, migrate, or contract)
func (dr *DeploymentRunner) applyPhase(deployment Deployment, phaseName string, script *ScriptFile, sqlFiles []SQLFile, isHead bool) error {
	// Execute shell script before SQL (if exists)
	if script != nil {
		if err := dr.executeScript(script, deployment, phaseName, isHead); err != nil {
			return fmt.Errorf("script execution failed: %w", err)
		}
	}

	// Apply SQL files if they exist
	if err := dr.applySQLFiles(sqlFiles, phaseName); err != nil {
		return fmt.Errorf("SQL execution failed: %w", err)
	}

	return nil
}

// applyPostPhase applies the post-validation phase
func (dr *DeploymentRunner) applyPostPhase(deployment Deployment, isHead bool) error {
	if deployment.PostScript != nil {
		if err := dr.executeScript(deployment.PostScript, deployment, "post", isHead); err != nil {
			return fmt.Errorf("post-validation failed: %w", err)
		}
	}
	return nil
}

// executeScript executes a shell script with ZDD environment variables
func (dr *DeploymentRunner) executeScript(script *ScriptFile, deployment Deployment, phase string, isHead bool) error {
	// Set environment variables
	env := map[string]string{
		"ZDD_IS_HEAD":          fmt.Sprintf("%t", isHead),
		"ZDD_DEPLOYMENT_ID":    deployment.ID,
		"ZDD_DEPLOYMENT_NAME":  deployment.Name,
		"ZDD_PHASE":            phase,
		"ZDD_DEPLOYMENTS_PATH": dr.deploymentsPath,
		"ZDD_DATABASE_URL":     dr.db.ConnectionString(),
	}

	fmt.Printf("  Executing %s script: %s\n", phase, script.Path)

	if err := dr.executor.ExecuteCommandWithEnv(script.Path, deployment.Directory, env); err != nil {
		return fmt.Errorf("failed to execute script: %w", err)
	}

	return nil
}

// applySQLFiles applies a sequence of SQL files in order
func (dr *DeploymentRunner) applySQLFiles(sqlFiles []SQLFile, phase string) error {
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
		if err := dr.db.ExecuteSQLInTransaction([]string{sqlFile.Content}); err != nil {
			return fmt.Errorf("failed to execute %s SQL file %s: %w", phase, sqlFile.Path, err)
		}
	}
	return nil
}

// generateSchemaDiff generates and saves a diff of the schema changes
func (dr *DeploymentRunner) generateSchemaDiff(before, after string) error {
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
