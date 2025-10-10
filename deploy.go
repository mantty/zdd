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
)

type (
	// Deployment represents a single deployment with its expand/migrate/contract SQL files
	Deployment struct {
		ID               string
		Name             string
		CreatedAt        time.Time
		AppliedAt        *time.Time
		ExpandSQLFiles   []SQLFile
		MigrateSQLFiles  []SQLFile
		ContractSQLFiles []SQLFile
		ExpandScript     *ScriptFile
		MigrateScript    *ScriptFile
		ContractScript   *ScriptFile
		PostScript       *ScriptFile
		Directory        string
	}

	// SQLFile represents a single SQL file (expand/migrate/contract) with optional numbering
	SQLFile struct {
		Path     string
		Sequence int // For numbered files like expand.1.sql, expand.2.sql
		Content  string
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
		ExecuteSQLInTransaction(sqlStatements []string) error
		DumpSchema() (string, error)
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
	expandSQLPattern     = regexp.MustCompile(`^expand(?:\.(\d+))?\.sql$`)
	migrateSQLPattern    = regexp.MustCompile(`^migrate(?:\.(\d+))?\.sql$`)
	contractSQLPattern   = regexp.MustCompile(`^contract(?:\.(\d+))?\.sql$`)
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

	// Load default scripts from root deployments directory
	defaultExpandScript := loadScript(deploymentsPath, "expand.sh")
	defaultMigrateScript := loadScript(deploymentsPath, "migrate.sh")
	defaultContractScript := loadScript(deploymentsPath, "contract.sh")
	defaultPostScript := loadScript(deploymentsPath, "post.sh")

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

		// Apply default scripts as fallbacks
		if deployment.ExpandScript == nil && defaultExpandScript != nil {
			deployment.ExpandScript = defaultExpandScript
		}
		if deployment.MigrateScript == nil && defaultMigrateScript != nil {
			deployment.MigrateScript = defaultMigrateScript
		}
		if deployment.ContractScript == nil && defaultContractScript != nil {
			deployment.ContractScript = defaultContractScript
		}
		if deployment.PostScript == nil && defaultPostScript != nil {
			deployment.PostScript = defaultPostScript
		}

		deployments = append(deployments, *deployment)
	}

	// Sort deployments by ID (which is sequential)
	sort.Slice(deployments, func(i, j int) bool {
		return deployments[i].ID < deployments[j].ID
	})

	return deployments, nil
}

// loadSQLFiles loads SQL files matching a pattern from directory entries
func loadSQLFiles(deploymentPath string, entries []os.DirEntry, pattern *regexp.Regexp, errorContext string) ([]SQLFile, error) {
	var sqlFiles []SQLFile

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		matches := pattern.FindStringSubmatch(fileName)
		if matches == nil {
			continue
		}

		sequence := 0
		if matches[1] != "" {
			sequence, _ = strconv.Atoi(matches[1])
		}

		filePath := filepath.Join(deploymentPath, fileName)
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read %s file %s: %w", errorContext, filePath, err)
		}

		sqlFiles = append(sqlFiles, SQLFile{
			Path:     filePath,
			Sequence: sequence,
			Content:  string(content),
		})
	}

	// Sort by sequence
	sort.Slice(sqlFiles, func(i, j int) bool {
		return sqlFiles[i].Sequence < sqlFiles[j].Sequence
	})

	return sqlFiles, nil
}

// loadScript loads a shell script from a directory, returns nil if not found
func loadScript(dir, filename string) *ScriptFile {
	filePath := filepath.Join(dir, filename)
	if _, err := os.Stat(filePath); err != nil {
		// Script doesn't exist, which is fine
		return nil
	}

	return &ScriptFile{
		Path: filePath,
	}
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

	if deployment.ExpandSQLFiles, err = loadSQLFiles(deploymentPath, entries, expandSQLPattern, "expand SQL"); err != nil {
		return nil, err
	}
	if deployment.MigrateSQLFiles, err = loadSQLFiles(deploymentPath, entries, migrateSQLPattern, "migrate SQL"); err != nil {
		return nil, err
	}
	if deployment.ContractSQLFiles, err = loadSQLFiles(deploymentPath, entries, contractSQLPattern, "contract SQL"); err != nil {
		return nil, err
	}

	// Load shell scripts (deployment-specific)
	deployment.ExpandScript = loadScript(deploymentPath, "expand.sh")
	deployment.MigrateScript = loadScript(deploymentPath, "migrate.sh")
	deployment.ContractScript = loadScript(deploymentPath, "contract.sh")
	deployment.PostScript = loadScript(deploymentPath, "post.sh")

	return deployment, nil
}

// CreateDeployment creates a new deployment directory with the given name
func CreateDeployment(deploymentsPath, name string) (*Deployment, error) {
	if deploymentsPath == "" {
		deploymentsPath = deploymentsDir
	}

	// Sanitize name
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)

	// Find the next sequential ID by checking existing deployments
	existingDeployments, err := LoadDeployments(deploymentsPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load existing deployments: %w", err)
	}

	// Determine next ID
	nextID := 1
	for _, m := range existingDeployments {
		// Parse the ID as an integer
		if idNum, err := strconv.Atoi(m.ID); err == nil {
			if idNum >= nextID {
				nextID = idNum + 1
			}
		}
	}

	// Format ID as 6-digit zero-padded string
	id := fmt.Sprintf("%06d", nextID)
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
		{filepath.Join(deploymentPath, "expand.sql"), "-- Expand phase SQL (optional)\n-- Add new columns, tables, etc. that are backward compatible\n", 0644},
		{filepath.Join(deploymentPath, "migrate.sql"), "-- Migrate phase SQL (optional)\n-- Core schema changes, data transformations\n", 0644},
		{filepath.Join(deploymentPath, "contract.sql"), "-- Contract phase SQL (optional)\n-- Remove old columns, tables, etc. no longer needed\n", 0644},
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

	// Include expand SQL files
	for _, sqlFile := range deployment.ExpandSQLFiles {
		hasher.Write([]byte(sqlFile.Content))
	}

	// Include migrate SQL files
	for _, sqlFile := range deployment.MigrateSQLFiles {
		hasher.Write([]byte(sqlFile.Content))
	}

	// Include contract SQL files
	for _, sqlFile := range deployment.ContractSQLFiles {
		hasher.Write([]byte(sqlFile.Content))
	}

	return fmt.Sprintf("%x", hasher.Sum(nil))
}

// HasNonEmptySQL checks if a slice of SQL files contains non-empty SQL content
// It returns true if any file contains actual SQL statements (not just comments or whitespace)
func HasNonEmptySQL(sqlFiles []SQLFile) bool {
	for _, sqlFile := range sqlFiles {
		content := strings.TrimSpace(sqlFile.Content)
		if content != "" {
			// Check if there's actual SQL content beyond comments
			lines := strings.Split(content, "\n")
			for _, line := range lines {
				line = strings.TrimSpace(line)
				if line != "" && !strings.HasPrefix(line, "--") {
					return true
				}
			}
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
			if HasNonEmptySQL(d.ExpandSQLFiles) {
				phases = append(phases, "expand")
			}
			if HasNonEmptySQL(d.MigrateSQLFiles) {
				phases = append(phases, "migrate")
			}
			if HasNonEmptySQL(d.ContractSQLFiles) {
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
