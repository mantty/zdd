package zdd

import (
	"fmt"
	"os"
	"path/filepath"

	"github.com/goccy/go-yaml"
)

// ConfigLoader handles loading and merging of zdd.yaml configuration files
type ConfigLoader struct{}

// LoadMigrationConfig loads configuration for a specific migration
// It first loads the default config from the migrations root directory (parent of migrationDir),
// then loads the migration-specific config and merges them.
// Migration-specific config takes precedence, but only for defined values.
func (c *ConfigLoader) LoadMigrationConfig(migrationDir string) (*MigrationConfig, error) {
	// Derive migrations root from migration directory (parent directory)
	migrationsRoot := filepath.Dir(migrationDir)

	// Load default config from migrations root
	defaultConfig, err := c.loadConfigFromDir(migrationsRoot)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load default config: %w", err)
	}

	// Load migration-specific config
	migrationConfig, err := c.loadConfigFromDir(migrationDir)
	if err != nil && !os.IsNotExist(err) {
		return nil, fmt.Errorf("failed to load migration config: %w", err)
	}

	// Merge configs - migration-specific takes precedence
	merged := c.mergeConfigs(defaultConfig, migrationConfig)
	return merged, nil
}

// loadConfigFromDir loads zdd.yaml from the specified directory
func (c *ConfigLoader) loadConfigFromDir(dir string) (*MigrationConfig, error) {
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
func (c *ConfigLoader) mergeConfigs(defaultConfig, migrationConfig *MigrationConfig) *MigrationConfig {
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

// GenerateExampleConfig creates a commented example configuration file
func (c *ConfigLoader) GenerateExampleConfig() string {
	return `# Expand phase: prepare database for new schema (e.g., add new columns, tables)
# These changes should be backward compatible with the old application version
# expand: echo 'Running expand phase command...'

# Migrate phase: core schema changes (e.g., data transformations, constraints)
# Application should typically be stopped during this phase
# migrate: kubectl set image deployment/myapp myapp=myapp:latest

# Contract phase: remove old schema elements no longer needed
# Only run after confirming the new application version is working
# contract: echo 'Running contract phase command...'

# Post phase: validation and testing commands
# These commands verify the deployment was successful
# post: curl -f http://myapp/health
`
}
