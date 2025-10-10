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
	// Migration represents a single migration with its expand/migrate/contract SQL files
	Migration struct {
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

	// SQLFile represents a single SQL file (pre or post) with optional numbering
	SQLFile struct {
		Path     string
		Sequence int // For numbered files like pre.1.sql, pre.2.sql
		Content  string
	}

	// ScriptFile represents a shell script file
	ScriptFile struct {
		Path string
	}

	// MigrationStatus represents the status of migrations in the system
	MigrationStatus struct {
		Local   []Migration
		Applied []Migration
		Pending []Migration
		Missing []Migration // Migrations that exist in DB but not locally
	}

	// DBMigrationRecord represents a migration record in the zdd_migrations table
	DBMigrationRecord struct {
		ID        string
		Name      string
		AppliedAt time.Time
		Checksum  string // Optional: for integrity checking
	}

	// DatabaseProvider interface abstracts database operations
	DatabaseProvider interface {
		InitMigrationSchema() error
		GetAppliedMigrations() ([]DBMigrationRecord, error)
		GetLastAppliedMigration() (*DBMigrationRecord, error)
		RecordMigration(migration Migration, checksum string) error
		ExecuteSQLInTransaction(sqlStatements []string) error
		DumpSchema() (string, error)
		ConnectionString() string
		Close() error
	}
)

const (
	migrationDirDefault = "migrations"
)

var (
	// Regex patterns for migration files
	migrationFilePattern = regexp.MustCompile(`^(\d{6})_(.+)$`)
	expandSQLPattern     = regexp.MustCompile(`^expand(?:\.(\d+))?\.sql$`)
	migrateSQLPattern    = regexp.MustCompile(`^migrate(?:\.(\d+))?\.sql$`)
	contractSQLPattern   = regexp.MustCompile(`^contract(?:\.(\d+))?\.sql$`)
)

// LoadMigrations scans the migrations directory and loads all migrations
func LoadMigrations(migrationsPath string) ([]Migration, error) {
	if migrationsPath == "" {
		migrationsPath = migrationDirDefault
	}

	if _, err := os.Stat(migrationsPath); os.IsNotExist(err) {
		return []Migration{}, nil // Return empty if migrations directory doesn't exist
	}

	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read migrations directory: %w", err)
	}

	// Load default scripts from root migrations directory
	defaultExpandScript := loadScript(migrationsPath, "expand.sh")
	defaultMigrateScript := loadScript(migrationsPath, "migrate.sh")
	defaultContractScript := loadScript(migrationsPath, "contract.sh")
	defaultPostScript := loadScript(migrationsPath, "post.sh")

	migrationDirs := make(map[string]string) // id -> directory name
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}

		matches := migrationFilePattern.FindStringSubmatch(entry.Name())
		if len(matches) != 3 {
			continue // Skip directories that don't match migration pattern
		}

		id := matches[1]
		migrationDirs[id] = entry.Name()
	}

	var migrations []Migration
	for id, dirName := range migrationDirs {
		migration, err := loadMigration(migrationsPath, id, dirName)
		if err != nil {
			return nil, fmt.Errorf("failed to load migration %s: %w", id, err)
		}

		// Apply default scripts as fallbacks
		if migration.ExpandScript == nil && defaultExpandScript != nil {
			migration.ExpandScript = defaultExpandScript
		}
		if migration.MigrateScript == nil && defaultMigrateScript != nil {
			migration.MigrateScript = defaultMigrateScript
		}
		if migration.ContractScript == nil && defaultContractScript != nil {
			migration.ContractScript = defaultContractScript
		}
		if migration.PostScript == nil && defaultPostScript != nil {
			migration.PostScript = defaultPostScript
		}

		migrations = append(migrations, *migration)
	}

	// Sort migrations by ID (which is timestamp-based)
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].ID < migrations[j].ID
	})

	return migrations, nil
}

// loadSQLFiles loads SQL files matching a pattern from directory entries
func loadSQLFiles(migrationPath string, entries []os.DirEntry, pattern *regexp.Regexp, errorContext string) ([]SQLFile, error) {
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

		filePath := filepath.Join(migrationPath, fileName)
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

// loadMigration loads a single migration from its directory
func loadMigration(migrationsPath, id, dirName string) (*Migration, error) {
	migrationPath := filepath.Join(migrationsPath, dirName)

	// Extract name from directory name
	matches := migrationFilePattern.FindStringSubmatch(dirName)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid migration directory name: %s", dirName)
	}

	migration := &Migration{
		ID:        id,
		Name:      matches[2],
		CreatedAt: time.Time{}, // Sequential IDs don't encode creation time
		Directory: migrationPath,
	}

	// Load SQL files
	entries, err := os.ReadDir(migrationPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory %s: %w", migrationPath, err)
	}

	if migration.ExpandSQLFiles, err = loadSQLFiles(migrationPath, entries, expandSQLPattern, "expand SQL"); err != nil {
		return nil, err
	}
	if migration.MigrateSQLFiles, err = loadSQLFiles(migrationPath, entries, migrateSQLPattern, "migrate SQL"); err != nil {
		return nil, err
	}
	if migration.ContractSQLFiles, err = loadSQLFiles(migrationPath, entries, contractSQLPattern, "contract SQL"); err != nil {
		return nil, err
	}

	// Load shell scripts (migration-specific)
	migration.ExpandScript = loadScript(migrationPath, "expand.sh")
	migration.MigrateScript = loadScript(migrationPath, "migrate.sh")
	migration.ContractScript = loadScript(migrationPath, "contract.sh")
	migration.PostScript = loadScript(migrationPath, "post.sh")

	return migration, nil
}

// CreateMigration creates a new migration directory with the given name
func CreateMigration(migrationsPath, name string) (*Migration, error) {
	if migrationsPath == "" {
		migrationsPath = migrationDirDefault
	}

	// Sanitize name
	name = strings.ReplaceAll(name, " ", "_")
	name = strings.ToLower(name)

	// Find the next sequential ID by checking existing migrations
	existingMigrations, err := LoadMigrations(migrationsPath)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load existing migrations: %w", err)
	}

	// Determine next ID
	nextID := 1
	for _, m := range existingMigrations {
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
	migrationPath := filepath.Join(migrationsPath, dirName)

	// Create migrations directory if it doesn't exist
	if err := os.MkdirAll(migrationsPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create migrations directory: %w", err)
	}

	// Create migration directory
	if err := os.MkdirAll(migrationPath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create migration directory: %w", err)
	}

	// Define migration files to create
	files := []struct {
		path    string
		content string
		mode    os.FileMode
	}{
		{filepath.Join(migrationPath, "expand.sql"), "-- Expand phase SQL (optional)\n-- Add new columns, tables, etc. that are backward compatible\n", 0644},
		{filepath.Join(migrationPath, "migrate.sql"), "-- Migrate phase SQL (optional)\n-- Core schema changes, data transformations\n", 0644},
		{filepath.Join(migrationPath, "contract.sql"), "-- Contract phase SQL (optional)\n-- Remove old columns, tables, etc. no longer needed\n", 0644},
		{filepath.Join(migrationPath, "expand.sh"), expandScriptTemplate, 0755},
		{filepath.Join(migrationPath, "migrate.sh"), migrateScriptTemplate, 0755},
		{filepath.Join(migrationPath, "contract.sh"), contractScriptTemplate, 0755},
		{filepath.Join(migrationPath, "post.sh"), postScriptTemplate, 0755},
	}

	// Create all migration files
	for _, f := range files {
		if err := os.WriteFile(f.path, []byte(f.content), f.mode); err != nil {
			return nil, fmt.Errorf("failed to create %s: %w", filepath.Base(f.path), err)
		}
	}

	migration := &Migration{
		ID:        id,
		Name:      name,
		CreatedAt: time.Now(),
		Directory: migrationPath,
	}

	return migration, nil
}

// CompareMigrations compares local migrations with applied migrations and returns status
func CompareMigrations(local []Migration, applied []DBMigrationRecord) *MigrationStatus {
	appliedMap := make(map[string]DBMigrationRecord)
	for _, m := range applied {
		appliedMap[m.ID] = m
	}

	localMap := make(map[string]Migration)
	for _, m := range local {
		localMap[m.ID] = m
	}

	status := &MigrationStatus{
		Local:   local,
		Applied: make([]Migration, 0),
		Pending: make([]Migration, 0),
		Missing: make([]Migration, 0),
	}

	// Classify local migrations
	for _, migration := range local {
		if appliedRecord, exists := appliedMap[migration.ID]; exists {
			// Migration has been applied
			migration.AppliedAt = &appliedRecord.AppliedAt
			status.Applied = append(status.Applied, migration)
		} else {
			// Migration is pending
			status.Pending = append(status.Pending, migration)
		}
	}

	// Find migrations that exist in DB but not locally
	for _, appliedRecord := range applied {
		if _, exists := localMap[appliedRecord.ID]; !exists {
			// Create a migration struct for the missing migration
			missingMigration := Migration{
				ID:        appliedRecord.ID,
				Name:      appliedRecord.Name,
				CreatedAt: time.Time{}, // No creation time available for missing migrations
				AppliedAt: &appliedRecord.AppliedAt,
			}
			status.Missing = append(status.Missing, missingMigration)
		}
	}

	return status
}

// CalculateChecksum calculates a checksum for a migration based on its SQL content
func CalculateChecksum(migration Migration) string {
	hasher := sha256.New()

	// Include expand SQL files
	for _, sqlFile := range migration.ExpandSQLFiles {
		hasher.Write([]byte(sqlFile.Content))
	}

	// Include migrate SQL files
	for _, sqlFile := range migration.MigrateSQLFiles {
		hasher.Write([]byte(sqlFile.Content))
	}

	// Include contract SQL files
	for _, sqlFile := range migration.ContractSQLFiles {
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

// ListMigrations loads migrations, optionally compares with database, and outputs a formatted status report
func ListMigrations(migrationsPath string, db DatabaseProvider) error {
	// Load local migrations
	localMigrations, err := LoadMigrations(migrationsPath)
	if err != nil {
		return fmt.Errorf("failed to load local migrations: %w", err)
	}

	// Get applied migrations from database if connected
	var appliedMigrations []DBMigrationRecord
	if db != nil {
		if err := db.InitMigrationSchema(); err != nil {
			return fmt.Errorf("failed to initialize migration schema: %w", err)
		}

		appliedMigrations, err = db.GetAppliedMigrations()
		if err != nil {
			return fmt.Errorf("failed to get applied migrations: %w", err)
		}
	}

	// Compare and display
	status := CompareMigrations(localMigrations, appliedMigrations)

	fmt.Println("Migration Status:")
	fmt.Println("================")

	if len(status.Applied) > 0 {
		fmt.Printf("\nApplied (%d):\n", len(status.Applied))
		for _, m := range status.Applied {
			fmt.Printf("  ✓ %s - %s (applied: %s)\n", m.ID, m.Name, m.AppliedAt.Format("2006-01-02 15:04:05"))
		}
	}

	if len(status.Pending) > 0 {
		fmt.Printf("\nPending (%d):\n", len(status.Pending))
		for _, m := range status.Pending {
			var phases []string
			if HasNonEmptySQL(m.ExpandSQLFiles) {
				phases = append(phases, "expand")
			}
			if HasNonEmptySQL(m.MigrateSQLFiles) {
				phases = append(phases, "migrate")
			}
			if HasNonEmptySQL(m.ContractSQLFiles) {
				phases = append(phases, "contract")
			}

			phaseInfo := ""
			if len(phases) > 0 {
				phaseInfo = fmt.Sprintf(" [%s]", strings.Join(phases, "+"))
			}
			fmt.Printf("  ○ %s - %s%s\n", m.ID, m.Name, phaseInfo)
		}
	}

	if len(status.Missing) > 0 {
		fmt.Printf("\nMissing Locally (%d):\n", len(status.Missing))
		for _, m := range status.Missing {
			fmt.Printf("  ! %s - %s (applied: %s)\n", m.ID, m.Name, m.AppliedAt.Format("2006-01-02 15:04:05"))
		}
	}

	if len(status.Pending) == 0 && len(status.Missing) == 0 {
		fmt.Println("\nAll migrations are up to date!")
	}

	return nil
}
