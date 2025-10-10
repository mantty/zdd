package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"

	"github.com/mantty/zdd"
	"github.com/mantty/zdd/postgres"
	"github.com/urfave/cli/v3"
)

const (
	version = "0.1.0"
)

func main() {
	ctx := context.Background()

	cmd := &cli.Command{
		Name:    "zdd",
		Usage:   "Zero Downtime Deployments - SQL migrations and app deployments",
		Version: version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "database-url",
				Aliases: []string{"d"},
				Usage:   "PostgreSQL connection string",
				Sources: cli.EnvVars("ZDD_DATABASE_URL"),
			},
			&cli.StringFlag{
				Name:    "migrations-path",
				Aliases: []string{"m"},
				Usage:   "Path to migrations directory",
				Value:   "migrations",
				Sources: cli.EnvVars("ZDD_MIGRATIONS_PATH"),
			},
		},
		Commands: []*cli.Command{
			{
				Name:    "create",
				Aliases: []string{"new"},
				Usage:   "Create a new migration",
				Arguments: []cli.Argument{
					&cli.StringArg{
						Name:      "name",
						UsageText: "NAME",
						Config: cli.StringConfig{
							TrimSpace: true,
						},
					},
				},
				Action: createCommand,
			},
			{
				Name:   "list",
				Usage:  "List migrations and their status",
				Action: listCommand,
			},
			{
				Name:   "migrate",
				Usage:  "Apply pending migrations",
				Action: migrateCommand,
			},
		},
	}

	if err := cmd.Run(ctx, os.Args); err != nil {
		log.Fatal(err)
	}
}

func createCommand(ctx context.Context, cmd *cli.Command) error {
	name := cmd.StringArg("name")
	if name == "" {
		return fmt.Errorf("migration name is required")
	}

	migrationsPath := cmd.String("migrations-path")

	migration, err := zdd.CreateMigration(migrationsPath, name)
	if err != nil {
		return fmt.Errorf("failed to create migration: %w", err)
	}

	fmt.Printf("Created migration %s\n", migration.Directory)

	return nil
}

func listCommand(ctx context.Context, cmd *cli.Command) error {
	migrationsPath := cmd.String("migrations-path")
	databaseURL := cmd.String("database-url")

	// Convert relative migrations path to absolute
	var err error
	migrationsPath, err = resolveMigrationsPath(migrationsPath)
	if err != nil {
		return err
	}

	// Connect to database if URL provided
	var db zdd.DatabaseProvider
	if databaseURL != "" {
		db, err = newDatabase(ctx, databaseURL)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer db.Close()
	}

	return zdd.ListMigrations(migrationsPath, db)
}

func migrateCommand(ctx context.Context, cmd *cli.Command) error {
	migrationsPath := cmd.String("migrations-path")
	databaseURL := cmd.String("database-url")

	// Convert relative migrations path to absolute
	migrationsPath, err := resolveMigrationsPath(migrationsPath)
	if err != nil {
		return err
	}

	if databaseURL == "" {
		return fmt.Errorf("database URL is required for migrations")
	}

	// Connect to database
	db, err := newDatabase(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Initialize migration schema
	if err := db.InitMigrationSchema(); err != nil {
		return fmt.Errorf("failed to initialize migration schema: %w", err)
	}

	// Create runner
	executor := zdd.NewShellCommandExecutor(0) // Use default timeout
	runner := zdd.NewMigrationRunner(db, migrationsPath, executor)

	// Run migrations
	return runner.RunMigrations(ctx)
}

// resolveMigrationsPath converts a relative path to absolute, returns path unchanged if already absolute or empty
func resolveMigrationsPath(path string) (string, error) {
	if path != "" && !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("failed to resolve migrations path: %w", err)
		}
		return abs, nil
	}
	return path, nil
}

// newDatabase creates a new database connection
// Currently only supports PostgreSQL
func newDatabase(ctx context.Context, databaseURL string) (zdd.DatabaseProvider, error) {
	if databaseURL == "" {
		return nil, fmt.Errorf("database URL is required")
	}

	// For now, we only support PostgreSQL
	return postgres.NewDB(ctx, databaseURL)
}
