package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"strings"

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
			&cli.StringFlag{
				Name:    "deploy-command",
				Aliases: []string{"c"},
				Usage:   "Command to run for deployment (e.g., 'npm deploy')",
				Sources: cli.EnvVars("ZDD_DEPLOY_COMMAND"),
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
	config, err := getConfig(cmd)
	if err != nil {
		return err
	}

	// Load local migrations
	localMigrations, err := zdd.LoadMigrations(config.MigrationsPath)
	if err != nil {
		return fmt.Errorf("failed to load local migrations: %w", err)
	}

	// Connect to database and get applied migrations
	var appliedMigrations []zdd.DBMigrationRecord
	if config.DatabaseURL != "" {
		db, err := postgres.NewDB(ctx, config.DatabaseURL)
		if err != nil {
			return fmt.Errorf("failed to connect to database: %w", err)
		}
		defer db.Close()

		if err := db.InitMigrationSchema(); err != nil {
			return fmt.Errorf("failed to initialize migration schema: %w", err)
		}

		appliedMigrations, err = db.GetAppliedMigrations()
		if err != nil {
			return fmt.Errorf("failed to get applied migrations: %w", err)
		}
	}

	// Compare migrations
	status := zdd.CompareMigrations(localMigrations, appliedMigrations)

	// Display results
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
			hasExpandSQL := zdd.HasNonEmptySQL(m.ExpandSQLFiles)
			hasMigrateSQL := zdd.HasNonEmptySQL(m.MigrateSQLFiles)
			hasContractSQL := zdd.HasNonEmptySQL(m.ContractSQLFiles)

			var flags []string
			if hasExpandSQL {
				flags = append(flags, "expand")
			}
			if hasMigrateSQL {
				flags = append(flags, "migrate")
			}
			if hasContractSQL {
				flags = append(flags, "contract")
			}

			flagStr := ""
			if len(flags) > 0 {
				flagStr = fmt.Sprintf(" [%s]", strings.Join(flags, "+"))
			}

			fmt.Printf("  ○ %s - %s%s\n", m.ID, m.Name, flagStr)
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

func migrateCommand(ctx context.Context, cmd *cli.Command) error {
	config, err := getConfig(cmd)
	if err != nil {
		return err
	}

	if config.DatabaseURL == "" {
		return fmt.Errorf("database URL is required for migrations")
	}

	// Connect to database
	db, err := postgres.NewDB(ctx, config.DatabaseURL)
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
	runner := zdd.NewMigrationRunner(db, config.MigrationsPath, executor, config)

	// Run migrations
	return runner.RunMigrations(ctx)
}

func getConfig(cmd *cli.Command) (*zdd.Config, error) {
	databaseURL := cmd.String("database-url")
	migrationsPath := cmd.String("migrations-path")
	deployCommand := cmd.String("deploy-command")

	// Convert relative migrations path to absolute
	if migrationsPath != "" && !filepath.IsAbs(migrationsPath) {
		abs, err := filepath.Abs(migrationsPath)
		if err != nil {
			return nil, fmt.Errorf("failed to resolve migrations path: %w", err)
		}
		migrationsPath = abs
	}

	return &zdd.Config{
		DatabaseURL:    databaseURL,
		MigrationsPath: migrationsPath,
		DeployCommand:  deployCommand,
	}, nil
}
