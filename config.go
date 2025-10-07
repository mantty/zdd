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
// It first loads the default config from the migrations root directory,
// then loads the migration-specific config and merges them.
// Migration-specific config takes precedence, but only for defined values.
func (c *ConfigLoader) LoadMigrationConfig(migrationDir, migrationsRoot string) (*MigrationConfig, error) {
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

	// Merge Expand commands
	if len(migrationConfig.Expand) > 0 {
		merged.Expand = migrationConfig.Expand
	} else if len(defaultConfig.Expand) > 0 {
		merged.Expand = defaultConfig.Expand
	}

	// Merge Migrate commands
	if len(migrationConfig.Migrate) > 0 {
		merged.Migrate = migrationConfig.Migrate
	} else if len(defaultConfig.Migrate) > 0 {
		merged.Migrate = defaultConfig.Migrate
	}

	// Merge Contract commands
	if len(migrationConfig.Contract) > 0 {
		merged.Contract = migrationConfig.Contract
	} else if len(defaultConfig.Contract) > 0 {
		merged.Contract = defaultConfig.Contract
	}

	// Merge Post commands
	if len(migrationConfig.Post) > 0 {
		merged.Post = migrationConfig.Post
	} else if len(defaultConfig.Post) > 0 {
		merged.Post = defaultConfig.Post
	}

	return merged
}

// GenerateExampleConfig creates a commented example configuration file
func (c *ConfigLoader) GenerateExampleConfig() string {
	return `# Expand phase: prepare database for new schema (e.g., add new columns, tables)
# These changes should be backward compatible with the old application version
expand:
  # - echo 'Running expand phase commands...'
  # - npm run build
  # - docker build -t myapp:latest .

# Migrate phase: core schema changes (e.g., data transformations, constraints)
# Application should typically be stopped during this phase
migrate:
  # - echo 'Running migrate phase commands...'
  # - kubectl set image deployment/myapp myapp=myapp:latest
  # - kubectl rollout status deployment/myapp

# Contract phase: remove old schema elements no longer needed
# Only run after confirming the new application version is working
contract:
  # - echo 'Running contract phase commands...'
  # - kubectl delete deployment/myapp-old

# Post phase: validation and testing commands
# These commands verify the deployment was successful
post:
  # - echo 'Running post-deployment validation...'
  # - curl -f http://myapp/health
  # - npm run test:integration
`
}
