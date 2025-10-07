package zdd

import (
	"crypto/sha256"
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
	migrationDirDefault = "migrations"
	migrationTimeFormat = "20060102150405" // YYYYMMDDHHMMSS format for lexicographic sorting
)

var (
	// Regex patterns for migration files
	migrationFilePattern = regexp.MustCompile(`^(\d{14})_(.+)$`)
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
		name := matches[2]
		migrationDirs[id] = entry.Name()
		_ = name // We'll use this when creating Migration structs
	}

	var migrations []Migration
	for id, dirName := range migrationDirs {
		migration, err := loadMigration(migrationsPath, id, dirName)
		if err != nil {
			return nil, fmt.Errorf("failed to load migration %s: %w", id, err)
		}
		migrations = append(migrations, *migration)
	}

	// Sort migrations by ID (which is timestamp-based)
	sort.Slice(migrations, func(i, j int) bool {
		return migrations[i].ID < migrations[j].ID
	})

	return migrations, nil
}

// loadMigration loads a single migration from its directory
func loadMigration(migrationsPath, id, dirName string) (*Migration, error) {
	migrationPath := filepath.Join(migrationsPath, dirName)

	// Parse timestamp from ID
	createdAt, err := time.Parse(migrationTimeFormat, id)
	if err != nil {
		return nil, fmt.Errorf("invalid migration ID format %s: %w", id, err)
	}

	// Extract name from directory name
	matches := migrationFilePattern.FindStringSubmatch(dirName)
	if len(matches) != 3 {
		return nil, fmt.Errorf("invalid migration directory name: %s", dirName)
	}
	name := matches[2]

	migration := &Migration{
		ID:        id,
		Name:      name,
		CreatedAt: createdAt,
		Directory: migrationPath,
	}

	// Load expand, migrate, and contract SQL files
	entries, err := os.ReadDir(migrationPath)
	if err != nil {
		return nil, fmt.Errorf("failed to read migration directory %s: %w", migrationPath, err)
	}

	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}

		fileName := entry.Name()
		filePath := filepath.Join(migrationPath, fileName)

		// Check if it's an expand SQL file
		if matches := expandSQLPattern.FindStringSubmatch(fileName); matches != nil {
			sequence := 0
			if matches[1] != "" {
				sequence, _ = strconv.Atoi(matches[1])
			}

			content, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read expand SQL file %s: %w", filePath, err)
			}

			migration.ExpandSQLFiles = append(migration.ExpandSQLFiles, SQLFile{
				Path:     filePath,
				Sequence: sequence,
				Content:  string(content),
			})
		}

		// Check if it's a migrate SQL file
		if matches := migrateSQLPattern.FindStringSubmatch(fileName); matches != nil {
			sequence := 0
			if matches[1] != "" {
				sequence, _ = strconv.Atoi(matches[1])
			}

			content, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read migrate SQL file %s: %w", filePath, err)
			}

			migration.MigrateSQLFiles = append(migration.MigrateSQLFiles, SQLFile{
				Path:     filePath,
				Sequence: sequence,
				Content:  string(content),
			})
		}

		// Check if it's a contract SQL file
		if matches := contractSQLPattern.FindStringSubmatch(fileName); matches != nil {
			sequence := 0
			if matches[1] != "" {
				sequence, _ = strconv.Atoi(matches[1])
			}

			content, err := os.ReadFile(filePath)
			if err != nil {
				return nil, fmt.Errorf("failed to read contract SQL file %s: %w", filePath, err)
			}

			migration.ContractSQLFiles = append(migration.ContractSQLFiles, SQLFile{
				Path:     filePath,
				Sequence: sequence,
				Content:  string(content),
			})
		}
	}

	// Sort SQL files by sequence
	sort.Slice(migration.ExpandSQLFiles, func(i, j int) bool {
		return migration.ExpandSQLFiles[i].Sequence < migration.ExpandSQLFiles[j].Sequence
	})
	sort.Slice(migration.MigrateSQLFiles, func(i, j int) bool {
		return migration.MigrateSQLFiles[i].Sequence < migration.MigrateSQLFiles[j].Sequence
	})
	sort.Slice(migration.ContractSQLFiles, func(i, j int) bool {
		return migration.ContractSQLFiles[i].Sequence < migration.ContractSQLFiles[j].Sequence
	})

	// Load configuration
	config, err := LoadMigrationConfig(migrationPath)
	if err != nil {
		return nil, fmt.Errorf("failed to load migration config: %w", err)
	}
	migration.Config = config

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

	// Generate timestamp-based ID
	id := time.Now().Format(migrationTimeFormat)
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

	// Create empty expand.sql, migrate.sql, and contract.sql files
	expandSQLPath := filepath.Join(migrationPath, "expand.sql")
	migrateSQLPath := filepath.Join(migrationPath, "migrate.sql")
	contractSQLPath := filepath.Join(migrationPath, "contract.sql")
	configPath := filepath.Join(migrationPath, "zdd.yaml")

	if err := os.WriteFile(expandSQLPath, []byte("-- Expand phase SQL (optional)\n-- Add new columns, tables, etc. that are backward compatible\n"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create expand.sql: %w", err)
	}

	if err := os.WriteFile(migrateSQLPath, []byte("-- Migrate phase SQL (optional)\n-- Core schema changes, data transformations\n"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create migrate.sql: %w", err)
	}

	if err := os.WriteFile(contractSQLPath, []byte("-- Contract phase SQL (optional)\n-- Remove old columns, tables, etc. no longer needed\n"), 0644); err != nil {
		return nil, fmt.Errorf("failed to create contract.sql: %w", err)
	}

	// Create example configuration file
	if err := os.WriteFile(configPath, []byte(ExampleConfigYAML), 0644); err != nil {
		return nil, fmt.Errorf("failed to create zdd.yaml: %w", err)
	}

	createdAt, _ := time.Parse(migrationTimeFormat, id)
	migration := &Migration{
		ID:        id,
		Name:      name,
		CreatedAt: createdAt,
		Directory: migrationPath,
		ExpandSQLFiles: []SQLFile{{
			Path:     expandSQLPath,
			Sequence: 0,
			Content:  "-- Expand phase SQL (optional)\n-- Add new columns, tables, etc. that are backward compatible\n",
		}},
		MigrateSQLFiles: []SQLFile{{
			Path:     migrateSQLPath,
			Sequence: 0,
			Content:  "-- Migrate phase SQL (optional)\n-- Core schema changes, data transformations\n",
		}},
		ContractSQLFiles: []SQLFile{{
			Path:     contractSQLPath,
			Sequence: 0,
			Content:  "-- Contract phase SQL (optional)\n-- Remove old columns, tables, etc. no longer needed\n",
		}},
		Config: &MigrationConfig{}, // Empty config initially
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
			createdAt, _ := time.Parse(migrationTimeFormat, appliedRecord.ID)
			missingMigration := Migration{
				ID:        appliedRecord.ID,
				Name:      appliedRecord.Name,
				CreatedAt: createdAt,
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

// HasNonEmptyExpandSQL checks if migration has non-empty expand SQL
func HasNonEmptyExpandSQL(migration Migration) bool {
	for _, sqlFile := range migration.ExpandSQLFiles {
		content := strings.TrimSpace(sqlFile.Content)
		if content != "" &&
			!strings.HasPrefix(content, "-- Expand phase SQL (optional)") {
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

// HasNonEmptyMigrateSQL checks if migration has non-empty migrate SQL
func HasNonEmptyMigrateSQL(migration Migration) bool {
	for _, sqlFile := range migration.MigrateSQLFiles {
		content := strings.TrimSpace(sqlFile.Content)
		if content != "" &&
			!strings.HasPrefix(content, "-- Migrate phase SQL (optional)") {
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

// HasNonEmptyContractSQL checks if migration has non-empty contract SQL
func HasNonEmptyContractSQL(migration Migration) bool {
	for _, sqlFile := range migration.ContractSQLFiles {
		content := strings.TrimSpace(sqlFile.Content)
		if content != "" &&
			!strings.HasPrefix(content, "-- Contract phase SQL (optional)") {
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

// ValidateOutstandingMigrations validates that there's at most one migration with expand/migrate/contract
// after the last applied migration
func ValidateOutstandingMigrations(pending []Migration) error {
	migrationsWithExpandContract := 0

	for _, migration := range pending {
		hasExpandSQL := HasNonEmptyExpandSQL(migration)
		hasMigrateSQL := HasNonEmptyMigrateSQL(migration)
		hasContractSQL := HasNonEmptyContractSQL(migration)

		if hasExpandSQL || hasMigrateSQL || hasContractSQL {
			migrationsWithExpandContract++
		}
	}

	if migrationsWithExpandContract > 1 {
		return fmt.Errorf("found %d pending migrations with expand/migrate/contract SQL - only one is allowed", migrationsWithExpandContract)
	}

	return nil
}
