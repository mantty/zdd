package zdd

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/exec"
	"strings"
	"time"
)

const defaultScriptTimeout = 5 * time.Minute

type (
	Task struct {
		TaskType   string // 'sql' or 'script'
		Path       string // Path to the file to execute
		Phase      string // Phase name (e.g., 'expand', 'migrate', 'contract', 'post')
		Deployment *Deployment
	}

	Plan struct {
		Tasks           []Task
		AlreadyDeployed map[string]bool // Key is the DeploymentID, true if the deployment already exists in the remote DB
		db              DatabaseProvider
		deploymentsPath string
	}
)

// BuildPlan creates a Plan by loading deployments and determining what needs to be applied
func BuildPlan(deploymentsPath string, db DatabaseProvider) (*Plan, error) {
	// Load local deployments
	localDeployments, err := LoadDeployments(deploymentsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load local deployments: %w", err)
	}

	// Get applied deployments from DB
	appliedDeployments, err := db.GetAppliedDeployments()
	if err != nil {
		return nil, fmt.Errorf("failed to get applied deployments: %w", err)
	}

	// Build map of already deployed
	alreadyDeployed := make(map[string]bool)
	for _, applied := range appliedDeployments {
		alreadyDeployed[applied.ID] = true
	}

	// Build tasks from deployments - just collect what each deployment provides
	var tasks []Task
	for _, deployment := range localDeployments {
		if !alreadyDeployed[deployment.ID] {
			tasks = append(tasks, deployment.Tasks()...)
		}
	}

	return &Plan{
		Tasks:           tasks,
		AlreadyDeployed: alreadyDeployed,
		db:              db,
		deploymentsPath: deploymentsPath,
	}, nil
}

// Execute applies the plan by executing all tasks in order
func (p *Plan) Execute() error {
	if len(p.Tasks) == 0 {
		fmt.Println("No pending deployments to apply")
		return nil
	}

	// Determine which deployment is the head (last pending)
	// Since BuildPlan only includes tasks from pending deployments,
	// the last task belongs to the last pending deployment
	var lastPendingID string
	if len(p.Tasks) > 0 {
		lastPendingID = p.Tasks[len(p.Tasks)-1].Deployment.ID
	}

	// Track which deployments we've started and completed
	startedDeployments := make(map[string]bool)
	completedDeployments := make(map[string]*Deployment)

	for _, task := range p.Tasks {
		// Check if this deployment is already applied (skip entire deployment)
		if p.AlreadyDeployed[task.Deployment.ID] {
			continue
		}

		if task.Deployment == nil {
			return fmt.Errorf("task %s missing deployment metadata", task.Path)
		}
		deployment := task.Deployment
		isHead := task.Deployment.ID == lastPendingID

		// Print deployment header when we first encounter it
		if !startedDeployments[task.Deployment.ID] {
			fmt.Printf("Applying deployment %s: %s\n", deployment.ID, deployment.Name)
			startedDeployments[task.Deployment.ID] = true
		}

		// Execute the task based on its type
		switch task.TaskType {
		case "script":
			if err := p.ExecuteScript(task.Path, *deployment, task.Phase, isHead); err != nil {
				return fmt.Errorf("failed to execute %s script for deployment %s: %w", task.Phase, task.Deployment.ID, err)
			}

		case "sql":
			// Read SQL file content
			content, err := os.ReadFile(task.Path)
			if err != nil {
				return fmt.Errorf("failed to read SQL file %s: %w", task.Path, err)
			}

			fmt.Printf("  Executing %s SQL file: %s\n", task.Phase, task.Path)
			if err := p.db.ExecuteSQLInTransaction(string(content)); err != nil {
				return fmt.Errorf("failed to execute %s SQL file %s: %w", task.Phase, task.Path, err)
			}

		default:
			return fmt.Errorf("unknown task type: %s", task.TaskType)
		}

		// Mark deployment as completed
		completedDeployments[task.Deployment.ID] = deployment
	}

	// Record all completed deployments to the database
	for deploymentID, deployment := range completedDeployments {
		checksum := CalculateChecksum(*deployment)
		if err := p.db.RecordDeployment(*deployment, checksum); err != nil {
			return fmt.Errorf("failed to record deployment %s: %w", deploymentID, err)
		}
		fmt.Printf("Deployment %s applied successfully\n", deploymentID)
	}

	fmt.Println("All deployments applied successfully!")
	return nil
}

// ExecuteScript executes a shell script with ZDD environment variables
func (p *Plan) ExecuteScript(scriptPath string, deployment Deployment, phase string, isHead bool) error {
	if strings.TrimSpace(scriptPath) == "" {
		return nil
	}

	// Set environment variables
	env := map[string]string{
		"ZDD_IS_HEAD":          fmt.Sprintf("%t", isHead),
		"ZDD_DEPLOYMENT_ID":    deployment.ID,
		"ZDD_DEPLOYMENT_NAME":  deployment.Name,
		"ZDD_PHASE":            phase,
		"ZDD_DEPLOYMENTS_PATH": p.deploymentsPath,
		"ZDD_DATABASE_URL":     p.db.ConnectionString(),
	}

	fmt.Printf("  Executing %s script: %s\n", phase, scriptPath)
	log.Printf("Executing script in directory: %s", deployment.Directory)
	log.Printf("Running script: %s", scriptPath)

	ctx, cancel := context.WithTimeout(context.Background(), defaultScriptTimeout)
	defer cancel()

	// Execute the script directly
	cmd := exec.CommandContext(ctx, scriptPath)
	cmd.Dir = deployment.Directory

	// Set environment variables
	cmd.Env = append(cmd.Environ(), []string{}...)
	for key, value := range env {
		cmd.Env = append(cmd.Env, fmt.Sprintf("%s=%s", key, value))
		log.Printf("Setting env: %s=%s", key, value)
	}

	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("script timed out after %v", defaultScriptTimeout)
		}
		log.Printf("Script output: %s", string(output))
		return fmt.Errorf("script failed with exit code %d: %s", cmd.ProcessState.ExitCode(), string(output))
	}

	// Log script output if there is any
	if len(output) > 0 {
		log.Printf("Script output: %s", string(output))
	}

	log.Printf("Script completed successfully")
	return nil
}
