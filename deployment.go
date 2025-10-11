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
)

type (
	// Deployment represents a single deployment with its expand/migrate/contract SQL files
	Deployment struct {
		ID              string
		Name            string
		CreatedAt       time.Time
		AppliedAt       *time.Time
		ExpandSQLFile   *SQLFile
		MigrateSQLFile  *SQLFile
		ContractSQLFile *SQLFile
		ExpandScript    *ScriptFile
		MigrateScript   *ScriptFile
		ContractScript  *ScriptFile
		PostScript      *ScriptFile
		Directory       string
	}

	// SQLFile represents a single SQL file (expand/migrate/contract)
	SQLFile struct {
		Path    string
		Content string
	}

	// ScriptFile represents a shell script file
	ScriptFile struct {
		Path string
	}

	// DeploymentStatus represents the status of deployments in the system
	DeploymentStatus struct {
		Local   []Deployment
		Applied []Deployment
		Pending []Deployment
		Missing []Deployment // Deployments that exist in DB but not locally
	}

	// DBDeploymentRecord represents a deployment record in the zdd_deployments table
	DBDeploymentRecord struct {
		ID        string
		Name      string
		AppliedAt time.Time
		Checksum  string // Optional: for integrity checking
	}

	// DatabaseProvider interface abstracts database operations
	DatabaseProvider interface {
		InitDeploymentSchema() error
		GetAppliedDeployments() ([]DBDeploymentRecord, error)
		GetLastAppliedDeployment() (*DBDeploymentRecord, error)
		RecordDeployment(deployment Deployment, checksum string) error
		ExecuteSQLInTransaction(sqlStatements ...string) error
		DumpSchema() (string, error)
		GenerateSchemaDiff(before, after string) error
		ConnectionString() string
		Close() error
	}
)

const (
	deploymentsDir = "migrations"
)

var (
	// Regex patterns for deployment directory files
	deploymentDirPattern = regexp.MustCompile(`^(\d{6})_(.+)$`)
	expandSQLPattern     = regexp.MustCompile(`^expand\.sql$`)
	migrateSQLPattern    = regexp.MustCompile(`^migrate\.sql$`)
	contractSQLPattern   = regexp.MustCompile(`^contract\.sql$`)
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

// loadSQLFile loads a single SQL file matching a pattern from directory entries
func loadSQLFile(deploymentPath string, entries []os.DirEntry, pattern *regexp.Regexp, errorContext string) (*SQLFile, error) {
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		if !pattern.MatchString(fileName) {
			continue
		}

		filePath := filepath.Join(deploymentPath, fileName)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s file %s: %w", errorContext, filePath, err)
		}

		sqlFile := &SQLFile{
			Path:    filePath,
			Content: string(content),
		}

		// If the file is empty or contains only comments/whitespace, treat it as if it doesn't exist
		if !IsNonEmptySQL(sqlFile) {
			return nil, nil
		}

		return sqlFile, nil
	}

	return nil, nil
}

// loadScript loads a shell script from a directory, returns nil if not found
// If the script is not found in the specified directory, it will check the deploymentsPath for a default script
func loadScript(dir, filename, deploymentsPath string) *ScriptFile {
	filePath := filepath.Join(dir, filename)
	if _, err := os.Stat(filePath); err == nil {
		// Script exists in the deployment directory
		return &ScriptFile{
			Path: filePath,
		}
	}

	// Script doesn't exist in deployment directory, check for default script
	// Only check if we're not already looking in the deployments root
	if dir != deploymentsPath {
		defaultPath := filepath.Join(deploymentsPath, filename)
		if _, err := os.Stat(defaultPath); err == nil {
			return &ScriptFile{
				Path: defaultPath,
			}
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
		CreatedAt: time.Time{}, // Sequential IDs don't encode creation time
		Directory: deploymentPath,
	}

	// Load SQL files
	entries, err := os.ReadDir(deploymentPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read deployment directory %s: %w", deploymentPath, err)
	}

	if deployment.ExpandSQLFile, err = loadSQLFile(deploymentPath, entries, expandSQLPattern, "expand SQL"); err != nil {
		return nil, err
	}
	if deployment.MigrateSQLFile, err = loadSQLFile(deploymentPath, entries, migrateSQLPattern, "migrate SQL"); err != nil {
		return nil, err
	}
	if deployment.ContractSQLFile, err = loadSQLFile(deploymentPath, entries, contractSQLPattern, "contract SQL"); err != nil {
		return nil, err
	}

	// Load shell scripts (deployment-specific, with fallback to defaults in deploymentsPath)
	deployment.ExpandScript = loadScript(deploymentPath, "expand.sh", deploymentsPath)
	deployment.MigrateScript = loadScript(deploymentPath, "migrate.sh", deploymentsPath)
	deployment.ContractScript = loadScript(deploymentPath, "contract.sh", deploymentsPath)
	deployment.PostScript = loadScript(deploymentPath, "post.sh", deploymentsPath)

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
		CreatedAt: time.Now(),
		Directory: deploymentPath,
	}

	return deployment, nil
}

// CompareDeployments compares local deployments with applied deployments and returns status
func CompareDeployments(local []Deployment, applied []DBDeploymentRecord) *DeploymentStatus {
	appliedMap := make(map[string]DBDeploymentRecord)
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
				CreatedAt: time.Time{}, // No creation time available for missing deployments
				AppliedAt: &appliedRecord.AppliedAt,
			}
			status.Missing = append(status.Missing, missingDeployment)
		}
	}

	return status
}

// CalculateChecksum calculates a checksum for a deployment based on its SQL content
func CalculateChecksum(deployment Deployment) string {
	hasher := sha256.New()

	// Include expand SQL file
	if deployment.ExpandSQLFile != nil {
		hasher.Write([]byte(deployment.ExpandSQLFile.Content))
	}

	// Include migrate SQL file
	if deployment.MigrateSQLFile != nil {
		hasher.Write([]byte(deployment.MigrateSQLFile.Content))
	}

	// Include contract SQL file
	if deployment.ContractSQLFile != nil {
		hasher.Write([]byte(deployment.ContractSQLFile.Content))
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// Tasks returns all tasks for this deployment in execution order
func (d Deployment) Tasks() []Task {
	var tasks []Task
	deployment := d

	// Define phases in order with their associated scripts and SQL files
	phases := []struct {
		name    string
		script  *ScriptFile
		sqlFile *SQLFile
	}{
		{"expand", d.ExpandScript, d.ExpandSQLFile},
		{"migrate", d.MigrateScript, d.MigrateSQLFile},
		{"contract", d.ContractScript, d.ContractSQLFile},
		{"post", d.PostScript, nil},
	}

	for _, phase := range phases {
		// Add script task if script exists
		if phase.script != nil {
			tasks = append(tasks, Task{
				TaskType:   "script",
				Path:       phase.script.Path,
				Phase:      phase.name,
				Deployment: &deployment,
			})
		}

		// Add SQL task if SQL file exists and has non-empty content
		if phase.sqlFile != nil && IsNonEmptySQL(phase.sqlFile) {
			tasks = append(tasks, Task{
				TaskType:   "sql",
				Path:       phase.sqlFile.Path,
				Phase:      phase.name,
				Deployment: &deployment,
			})
		}
	}

	return tasks
}

// IsNonEmptySQL checks if a SQL file contains non-empty SQL content
// It returns true if the file contains actual SQL statements (not just comments or whitespace)
func IsNonEmptySQL(sqlFile *SQLFile) bool {
	if sqlFile == nil {
		return false
	}

	content := strings.TrimSpace(sqlFile.Content)
	if content == "" {
		return false
	}

	// Check if there's actual SQL content beyond comments
	lines := strings.Split(content, "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line != "" && !strings.HasPrefix(line, "--") {
			return true
		}
	}
	return false
}

// ListDeployments loads deployments, optionally compares with database, and outputs a formatted status report
func ListDeployments(deploymentsPath string, db DatabaseProvider) error {
	// Load local deployments
	localDeployments, err := LoadDeployments(deploymentsPath)
	if err != nil {
		return fmt.Errorf("failed to load local deployments: %w", err)
	}

	// Get applied deployments from database if connected
	var appliedDeployments []DBDeploymentRecord
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
			if IsNonEmptySQL(d.ExpandSQLFile) {
				phases = append(phases, "expand")
			}
			if IsNonEmptySQL(d.MigrateSQLFile) {
				phases = append(phases, "migrate")
			}
			if IsNonEmptySQL(d.ContractSQLFile) {
				phases = append(phases, "contract")
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
