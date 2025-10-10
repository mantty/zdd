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
				Name:    "deployments-path",
				Aliases: []string{"p"},
				Usage:   "Path to deployments directory",
				Value:   "migrations",
				Sources: cli.EnvVars("ZDD_DEPLOYMENTS_PATH"),
			},
		},
		Commands: []*cli.Command{
			{
				Name:  "create",
				Usage: "Create a new deployment",
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
				Usage:  "List deployments and their status",
				Action: listCommand,
			},
			{
				Name:   "deploy",
				Usage:  "Apply pending deployments",
				Action: deployCommand,
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
		return fmt.Errorf("deployment name is required")
	}

	deploymentsPath := cmd.String("deployments-path")

	deployment, err := zdd.CreateDeployment(deploymentsPath, name)
	if err != nil {
		return fmt.Errorf("failed to create deployment: %w", err)
	}

	fmt.Printf("Created deployment %s\n", deployment.Directory)

	return nil
}

func listCommand(ctx context.Context, cmd *cli.Command) error {
	deploymentsPath := cmd.String("deployments-path")
	databaseURL := cmd.String("database-url")

	// Convert relative deployments path to absolute
	var err error
	deploymentsPath, err = resolveDeploymentsPath(deploymentsPath)
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

	return zdd.ListDeployments(deploymentsPath, db)
}

func deployCommand(ctx context.Context, cmd *cli.Command) error {
	deploymentsPath := cmd.String("deployments-path")
	databaseURL := cmd.String("database-url")

	// Convert relative deployments path to absolute
	deploymentsPath, err := resolveDeploymentsPath(deploymentsPath)
	if err != nil {
		return err
	}

	if databaseURL == "" {
		return fmt.Errorf("database URL is required for deployments")
	}

	// Connect to database
	db, err := newDatabase(ctx, databaseURL)
	if err != nil {
		return fmt.Errorf("failed to connect to database: %w", err)
	}
	defer db.Close()

	// Initialize deployment schema
	if err := db.InitDeploymentSchema(); err != nil {
		return fmt.Errorf("failed to initialize deployment schema: %w", err)
	}

	// Create runner
	executor := zdd.NewShellCommandExecutor(0) // Use default timeout
	runner := zdd.NewDeploymentRunner(db, deploymentsPath, executor)

	// Run deployments
	return runner.RunDeployments(ctx)
}

// resolveDeploymentsPath converts a relative path to absolute, returns path unchanged if already absolute or empty
func resolveDeploymentsPath(path string) (string, error) {
	if path != "" && !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("failed to resolve deployments path: %w", err)
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
