package zdd_test

import (
	"fmt"
	"log"
	"path/filepath"
	"testing"

	"github.com/mantty/zdd"
)

func TestConfigLoading(t *testing.T) {
	// Test configuration loading for our simplified YAML structure
	migrationsPath := "./testdata/migrations_01"

	// Convert relative path to absolute
	abs, err := filepath.Abs(migrationsPath)
	if err != nil {
		log.Fatalf("Failed to resolve migrations path: %v", err)
	}

	// Load migrations to test YAML parsing
	migrations, err := zdd.LoadMigrations(abs)
	if err != nil {
		log.Fatalf("Failed to load migrations: %v", err)
	}

	for _, migration := range migrations {
		// Test loading the YAML configuration
		config, err := zdd.LoadMigrationConfig(migration.Directory)
		if err != nil {
			fmt.Printf("Error loading config: %v\n", err)
			continue
		}

		// Verify that the configuration has been parsed correctly
		if config.Expand == nil || *config.Expand == "" {
			t.Errorf("Expected expand command to be set")
		}
		if config.Migrate == nil || *config.Migrate == "" {
			t.Errorf("Expected migrate command to be set")
		}
		if config.Contract == nil || *config.Contract == "" {
			t.Errorf("Expected contract command to be set")
		}
		if config.Post == nil || *config.Post == "" {
			t.Errorf("Expected post command to be set")
		}
	}
}
