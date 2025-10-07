package zdd

import (
	_ "embed"
	"fmt"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

//go:embed assets/zdd.yaml
var ExampleConfigYAML string

// LoadMigrationConfig loads configuration for a specific migration
// It first loads the default config from the migrations root directory (parent of migrationDir),
// then loads the migration-specific config and merges them.
// Migration-specific config takes precedence, but only for defined values.
func LoadMigrationConfig(migrationDir string) (*MigrationConfig, error) {
	// Derive migrations root from migration directory (parent directory)
	migrationsRoot := filepath.Dir(migrationDir)

	// Load default config from migrations root
	defaultConfig, err := loadConfigFromDir(migrationsRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load default config: %w", err)
	}

	// Load migration-specific config
	migrationConfig, err := loadConfigFromDir(migrationDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Merge configs - migration-specific takes precedence
	merged := mergeConfigs(defaultConfig, migrationConfig)
	return merged, nil
}

// loadConfigFromDir loads zdd.yaml from the specified directory
func loadConfigFromDir(dir string) (*MigrationConfig, error) {
	configPath := filepath.Join(dir, "zdd.yaml")

	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, err
	}

	// Parse YAML
	var config MigrationConfig
	if err := yaml.Unmarshal(data, &config); err != nil {
		return nil, fmt.Errorf("failed to parse config file %s: %w", configPath, err)
	}

	return &config, nil
}

// mergeConfigs merges default and migration-specific configs
// Only uses default values when migration config values are nil (undefined)
func mergeConfigs(defaultConfig, migrationConfig *MigrationConfig) *MigrationConfig {
	if defaultConfig == nil && migrationConfig == nil {
		return &MigrationConfig{}
	}
	if defaultConfig == nil {
		return migrationConfig
	}
	if migrationConfig == nil {
		return defaultConfig
	}

	merged := &MigrationConfig{}

	// Merge Expand command
	if migrationConfig.Expand != nil {
		merged.Expand = migrationConfig.Expand
	} else if defaultConfig.Expand != nil {
		merged.Expand = defaultConfig.Expand
	}

	// Merge Migrate command
	if migrationConfig.Migrate != nil {
		merged.Migrate = migrationConfig.Migrate
	} else if defaultConfig.Migrate != nil {
		merged.Migrate = defaultConfig.Migrate
	}

	// Merge Contract command
	if migrationConfig.Contract != nil {
		merged.Contract = migrationConfig.Contract
	} else if defaultConfig.Contract != nil {
		merged.Contract = defaultConfig.Contract
	}

	// Merge Post command
	if migrationConfig.Post != nil {
		merged.Post = migrationConfig.Post
	} else if defaultConfig.Post != nil {
		merged.Post = defaultConfig.Post
	}

	return merged
}
