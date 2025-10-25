package zdd

import (
	"crypto/sha256"
	_ "embed"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	deploymentsDir = "migrations"
)

var (
	//go:embed assets/expand.sh
	expandScriptTemplate string

	//go:embed assets/migrate.sh
	migrateScriptTemplate string

	//go:embed assets/contract.sh
	contractScriptTemplate string

	//go:embed assets/post.sh
	postScriptTemplate string

	//go:embed assets/expand.sql
	expandSQLTemplate string

	//go:embed assets/migrate.sql
	migrateSQLTemplate string

	//go:embed assets/contract.sql
	contractSQLTemplate string

	// Regex pattern for deployment directory naming
	deploymentDirPattern = regexp.MustCompile(`^(\d{6})_(.+)$`)

	// Regex pattern for matching deployment sql and sh files
	deploymentFilePattern = regexp.MustCompile(`^(expand|migrate|contract|post)\.(sh|sql)$`)
)

type (
	// Deployment represents a single deployment with its expand/migrate/contract SQL files
	Deployment struct {
		ID        string
		Name      string
		AppliedAt *time.Time
		Phases    map[string]DeploymentPhase
		Directory string
	}

	// DeploymentDBRecord represents a deployment record in the zdd_deployments table
	DeploymentDBRecord struct {
		ID        string
		Name      string
		AppliedAt time.Time
		Checksum  string // Optional: for integrity checking
	}

	DeploymentPhase struct {
		ScriptFilePath *string
		SQLFilePath    *string
	}

	// DeploymentStatus represents the status of deployments in the system
	DeploymentStatus struct {
		Local   []Deployment
		Applied []Deployment
		Pending []Deployment
		Missing []Deployment // Deployments that exist in DB but not locally
	}

	// ScriptFile represents a shell script file
	ScriptFile struct {
		Path string
	}

	// SQLFile represents a single SQL file (expand/migrate/contract)
	SQLFile struct {
		Path string
	}

	// DatabaseProvider interface abstracts database operations
	DatabaseProvider interface {
		InitDeploymentSchema() error
		GetAppliedDeployments() ([]DeploymentDBRecord, error)
		GetLastAppliedDeployment() (*DeploymentDBRecord, error)
		RecordDeployment(deployment Deployment, checksum string) error
		ExecuteSQLInTransaction(sqlStatements ...string) error
		ConnectionString() string
		Close() error
	}
)

// LoadDeployments scans the deployments directory and loads all deployments
func LoadDeployments(deploymentsPath string) ([]Deployment, error) {
	if deploymentsPath == "" {
		deploymentsPath = deploymentsDir
	}

	if _, err := os.Stat(deploymentsPath); os.IsNotExist(err) {
		return []Deployment{}, nil // Return empty if deployments directory doesn't exist
	}

	entries, err := os.ReadDir(deploymentsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read deployments directory: %w", err)
	}

	deploymentDirs := make(map[string]string) // id -> directory name
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := deploymentDirPattern.FindStringSubmatch(entry.Name())
		if len(matches) != 3 {
			continue // Skip directories that don't match deployment pattern
		}

		id := matches[1]
		deploymentDirs[id] = entry.Name()
	}

	var deployments []Deployment
	for id, dirName := range deploymentDirs {
		deployment, err := loadDeployment(deploymentsPath, id, dirName)
		if err != nil {
			return nil, fmt.Errorf("failed to load deployment %s: %w", id, err)
		}

		deployments = append(deployments, *deployment)
	}

	// Sort deployments by ID (which is sequential)
	sort.Slice(deployments, func(i, j int) bool {
		return deployments[i].ID < deployments[j].ID
	})

	return deployments, nil
}

// loadFiles loads sql and script files for a deployment
func loadFiles(deployment *Deployment, deploymentPath string) error {
	entries, err := os.ReadDir(deploymentPath)
	if err != nil {
		return fmt.Errorf("failed to read deployment directory %s: %w", deploymentPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		name := entry.Name()
		matches := deploymentFilePattern.FindStringSubmatch(name)
		if len(matches) != 3 {
			continue
		}

		phase := matches[1]
		fileType := matches[2]
		filePath := filepath.Join(deploymentPath, name)

		deploymentPhase := deployment.Phases[phase]
		if fileType == "sql" {
			deploymentPhase.SQLFilePath = &filePath
			deployment.Phases[phase] = deploymentPhase
			continue
		}

		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("failed to read file info: %w", err)
		}

		if info.Mode()&0111 != 0 {
			deploymentPhase.ScriptFilePath = &filePath
			deployment.Phases[phase] = deploymentPhase
		}
	}

	return nil
}

// loadDeployment loads a single deployment from its directory
func loadDeployment(deploymentsPath, id, dirName string) (*Deployment, error) {
	deploymentPath := filepath.Join(deploymentsPath, dirName)

	// Extract name from directory name
	matches := deploymentDirPattern.FindStringSubmatch(dirName)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid deployment directory name: %s", dirName)
	}

	deployment := &Deployment{
		ID:        id,
		Name:      matches[2],
		Directory: deploymentPath,
		Phases:    make(map[string]DeploymentPhase),
	}

	if err := loadFiles(deployment, deploymentPath); err != nil {
		return nil, err
	}

	return deployment, nil
}

// getNextDeploymentID determines the next sequential deployment ID by checking existing deployment directories
func getNextDeploymentID(deploymentsPath string) (string, error) {
	// Check if deployments directory exists
	entries, err := os.ReadDir(deploymentsPath)
	if err != nil {
		if os.IsNotExist(err) {
			// No deployments directory, start with ID 1
			return "000001", nil
		}
		return "", fmt.Errorf("failed to read deployments directory: %w", err)
	}

	// Find the last deployment directory (entries are sorted, so last is highest)
	var lastID string
	for i := len(entries) - 1; i >= 0; i-- {
		entry := entries[i]
		if !entry.IsDir() {
			continue
		}

		// Extract ID from directory name (format: XXXXXX_name)
		matches := deploymentDirPattern.FindStringSubmatch(entry.Name())
		if len(matches) == 3 {
			lastID = matches[1]
			break
		}
	}

	// If no valid deployment directories found, start with ID 1
	if lastID == "" {
		return "000001", nil
	}

	// Parse the last ID and increment it
	idNum, err := strconv.Atoi(lastID)
	if err != nil {
		return "", fmt.Errorf("failed to parse deployment ID %s: %w", lastID, err)
	}

	// Format next ID as 6-digit zero-padded string
	return fmt.Sprintf("%06d", idNum+1), nil
}

// CreateDeployment creates a new deployment directory with the given name
func CreateDeployment(deploymentsPath, name string) (*Deployment, error) {
	if deploymentsPath == "" {
		deploymentsPath = deploymentsDir
	}

	// Sanitize name
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)

	// Get the next deployment ID
	id, err := getNextDeploymentID(deploymentsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to determine next deployment ID: %w", err)
	}

	dirName := fmt.Sprintf("%s_%s", id, name)
	deploymentPath := filepath.Join(deploymentsPath, dirName)

	// Create deployments directory if it doesn't exist
	if err := os.MkdirAll(deploymentsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create deployments directory: %w", err)
	}

	// Create deployment directory
	if err := os.MkdirAll(deploymentPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create deployment directory: %w", err)
	}

	// Define deployment files to create
	files := []struct {
		path    string
		content string
		mode    os.FileMode
	}{
		{filepath.Join(deploymentPath, "expand.sql"), expandSQLTemplate, 0644},
		{filepath.Join(deploymentPath, "migrate.sql"), migrateSQLTemplate, 0644},
		{filepath.Join(deploymentPath, "contract.sql"), contractSQLTemplate, 0644},
		{filepath.Join(deploymentPath, "expand.sh"), expandScriptTemplate, 0755},
		{filepath.Join(deploymentPath, "migrate.sh"), migrateScriptTemplate, 0755},
		{filepath.Join(deploymentPath, "contract.sh"), contractScriptTemplate, 0755},
		{filepath.Join(deploymentPath, "post.sh"), postScriptTemplate, 0755},
	}

	// Create all deployment files
	for _, f := range files {
		if err := os.WriteFile(f.path, []byte(f.content), f.mode); err != nil {
			return nil, fmt.Errorf("failed to create %s: %w", filepath.Base(f.path), err)
		}
	}

	deployment := &Deployment{
		ID:        id,
		Name:      name,
		Directory: deploymentPath,
	}

	return deployment, nil
}

// CompareDeployments compares local deployments with applied deployments and returns status
func CompareDeployments(local []Deployment, applied []DeploymentDBRecord) *DeploymentStatus {
	appliedMap := make(map[string]DeploymentDBRecord)
	for _, m := range applied {
		appliedMap[m.ID] = m
	}

	localMap := make(map[string]Deployment)
	for _, m := range local {
		localMap[m.ID] = m
	}

	status := &DeploymentStatus{
		Local:   local,
		Applied: make([]Deployment, 0),
		Pending: make([]Deployment, 0),
		Missing: make([]Deployment, 0),
	}

	// Classify local deployments
	for _, deployment := range local {
		if appliedRecord, exists := appliedMap[deployment.ID]; exists {
			// Deployment has been applied
			deployment.AppliedAt = &appliedRecord.AppliedAt
			status.Applied = append(status.Applied, deployment)
		} else {
			// Deployment is pending
			status.Pending = append(status.Pending, deployment)
		}
	}

	// Find deployments that exist in DB but not locally
	for _, appliedRecord := range applied {
		if _, exists := localMap[appliedRecord.ID]; !exists {
			// Create a deployment struct for the missing deployment
			missingDeployment := Deployment{
				ID:        appliedRecord.ID,
				Name:      appliedRecord.Name,
				AppliedAt: &appliedRecord.AppliedAt,
			}
			status.Missing = append(status.Missing, missingDeployment)
		}
	}

	return status
}

// CalculateChecksum calculates a checksum for a deployment based on its SQL file paths
// TODO: Implement checksum calculation based on file paths or content if needed
func CalculateChecksum(deployment Deployment) string {
	hasher := sha256.New()

	// Include SQL file paths from phases
	for phase, phaseData := range deployment.Phases {
		if phaseData.SQLFilePath != nil {
			hasher.Write([]byte(phase + ":" + *phaseData.SQLFilePath))
		}
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// Tasks returns all tasks for this deployment in execution order
func (d Deployment) Tasks() []Task {
	var tasks []Task
	deployment := d

	// Define phases in order: expand, migrate, contract, post
	phaseOrder := []string{"expand", "migrate", "contract", "post"}

	for _, phaseName := range phaseOrder {
		phaseData, exists := d.Phases[phaseName]
		if !exists {
			continue
		}

		// Add script task if script exists
		if phaseData.ScriptFilePath != nil {
			tasks = append(tasks, Task{
				TaskType:   "script",
				Path:       *phaseData.ScriptFilePath,
				Phase:      phaseName,
				Deployment: &deployment,
			})
		}

		// Add SQL task if SQL file exists (for expand, migrate, contract only)
		if phaseData.SQLFilePath != nil && phaseName != "post" {
			tasks = append(tasks, Task{
				TaskType:   "sql",
				Path:       *phaseData.SQLFilePath,
				Phase:      phaseName,
				Deployment: &deployment,
			})
		}
	}

	return tasks
}

// IsNonEmptySQL checks if a SQL file exists at the given path
// Since we no longer load content, this just checks file existence
func IsNonEmptySQL(sqlFilePath string) bool {
	if sqlFilePath == "" {
		return false
	}

	if _, err := os.Stat(sqlFilePath); err != nil {
		return false
	}

	return true
}

// ListDeployments loads deployments, optionally compares with database, and outputs a formatted status report
func ListDeployments(deploymentsPath string, db DatabaseProvider) error {
	// Load local deployments
	localDeployments, err := LoadDeployments(deploymentsPath)
	if err != nil {
		return fmt.Errorf("failed to load local deployments: %w", err)
	}

	// Get applied deployments from database if connected
	var appliedDeployments []DeploymentDBRecord
	if db != nil {
		if err := db.InitDeploymentSchema(); err != nil {
			return fmt.Errorf("failed to initialize deployment schema: %w", err)
		}

		appliedDeployments, err = db.GetAppliedDeployments()
		if err != nil {
			return fmt.Errorf("failed to get applied deployments: %w", err)
		}
	}

	// Compare and display
	status := CompareDeployments(localDeployments, appliedDeployments)

	fmt.Println("Deployment Status:")
	fmt.Println("==================")

	if len(status.Applied) > 0 {
		fmt.Printf("\nApplied (%d):\n", len(status.Applied))
		for _, d := range status.Applied {
			fmt.Printf("  ✓ %s - %s (applied: %s)\n", d.ID, d.Name, d.AppliedAt.Format("2006-01-02 15:04:05"))
		}
	}

	if len(status.Pending) > 0 {
		fmt.Printf("\nPending (%d):\n", len(status.Pending))
		for _, d := range status.Pending {
			var phases []string
			for _, phaseName := range []string{"expand", "migrate", "contract"} {
				if phaseData, exists := d.Phases[phaseName]; exists && phaseData.SQLFilePath != nil {
					if IsNonEmptySQL(*phaseData.SQLFilePath) {
						phases = append(phases, phaseName)
					}
				}
			}

			phaseInfo := ""
			if len(phases) > 0 {
				phaseInfo = fmt.Sprintf(" [%s]", strings.Join(phases, "+"))
			}
			fmt.Printf("  ○ %s - %s%s\n", d.ID, d.Name, phaseInfo)
		}
	}

	if len(status.Missing) > 0 {
		fmt.Printf("\nMissing Locally (%d):\n", len(status.Missing))
		for _, d := range status.Missing {
			fmt.Printf("  ! %s - %s (applied: %s)\n", d.ID, d.Name, d.AppliedAt.Format("2006-01-02 15:04:05"))
		}
	}

	if len(status.Pending) == 0 && len(status.Missing) == 0 {
		fmt.Println("\nAll deployments are up to date!")
	}

	return nil
}
