package zdd

import (
	"time"
)

// MigrationConfig represents the configuration for a migration step
type MigrationConfig struct {
	Expand   []string `yaml:"expand,omitempty"`
	Migrate  []string `yaml:"migrate,omitempty"`
	Contract []string `yaml:"contract,omitempty"`
	Post     []string `yaml:"post,omitempty"`
}

// StepConfig represents configuration for a single migration step
// Deprecated: Use direct []string arrays in MigrationConfig instead
type StepConfig struct {
	Commands []string `json:"commands,omitempty"`
}

// Migration represents a single migration with its expand/migrate/contract SQL files
type Migration struct {
	ID               string
	Name             string
	CreatedAt        time.Time
	AppliedAt        *time.Time
	ExpandSQLFiles   []SQLFile
	MigrateSQLFiles  []SQLFile
	ContractSQLFiles []SQLFile
	Directory        string
	Config           *MigrationConfig
}

// SQLFile represents a single SQL file (pre or post) with optional numbering
type SQLFile struct {
	Path     string
	Sequence int // For numbered files like pre.1.sql, pre.2.sql
	Content  string
}

// MigrationStatus represents the status of migrations in the system
type MigrationStatus struct {
	Local   []Migration
	Applied []Migration
	Pending []Migration
	Missing []Migration // Migrations that exist in DB but not locally
}

// Config holds configuration for the zdd tool
type Config struct {
	DatabaseURL    string
	MigrationsPath string
	DeployCommand  string
}

// DBMigrationRecord represents a migration record in the zdd_migrations table
type DBMigrationRecord struct {
	ID        string
	Name      string
	AppliedAt time.Time
	Checksum  string // Optional: for integrity checking
}

// DatabaseProvider interface abstracts database operations
type DatabaseProvider interface {
	InitMigrationSchema() error
	GetAppliedMigrations() ([]DBMigrationRecord, error)
	GetLastAppliedMigration() (*DBMigrationRecord, error)
	RecordMigration(migration Migration, checksum string) error
	ExecuteSQLInTransaction(sqlStatements []string) error
	DumpSchema() (string, error)
	Close() error
}

// CommandExecutor interface abstracts command execution
type CommandExecutor interface {
	ExecuteCommands(commands []string, workingDir string) error
}
