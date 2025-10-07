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

	mm := zdd.NewMigrationManager(abs)
	loader := &zdd.ConfigLoader{}

	// Load migrations to test YAML parsing
	migrations, err := mm.LoadMigrations()
	if err != nil {
		log.Fatalf("Failed to load migrations: %v", err)
	}

	for _, migration := range migrations {
		// Test loading the YAML configuration
		config, err := loader.LoadMigrationConfig(migration.Directory, abs)
		if err != nil {
			fmt.Printf("Error loading config: %v\n", err)
			continue
		}

		// Verify that the configuration has been parsed correctly
		if len(config.Expand) != 2 {
			t.Errorf("Expected 2 expand commands, got %d", len(config.Expand))
		}
		if len(config.Migrate) != 2 {
			t.Errorf("Expected 2 migrate commands, got %d", len(config.Migrate))
		}
		if len(config.Contract) != 2 {
			t.Errorf("Expected 2 contract commands, got %d", len(config.Contract))
		}
		if len(config.Post) != 2 {
			t.Errorf("Expected 2 post commands, got %d", len(config.Post))
		}
	}
}
