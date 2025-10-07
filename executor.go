package zdd

import (
	"context"
	"fmt"
	"log"
	"os/exec"
	"strings"
	"time"
)

// ShellCommandExecutor implements CommandExecutor for executing shell commands
type ShellCommandExecutor struct {
	timeout time.Duration
}

// NewShellCommandExecutor creates a new shell command executor
func NewShellCommandExecutor(timeout time.Duration) *ShellCommandExecutor {
	if timeout == 0 {
		timeout = 5 * time.Minute // Default timeout
	}
	return &ShellCommandExecutor{
		timeout: timeout,
	}
}

// ExecuteCommands executes a list of shell commands in sequence
func (e *ShellCommandExecutor) ExecuteCommands(commands []string, workingDir string) error {
	if len(commands) == 0 {
		return nil
	}

	log.Printf("Executing %d commands in directory: %s", len(commands), workingDir)

	for i, command := range commands {
		if strings.TrimSpace(command) == "" {
			continue
		}

		log.Printf("Running command %d/%d: %s", i+1, len(commands), command)

		if err := e.executeCommand(command, workingDir); err != nil {
			return fmt.Errorf("command %d failed (%s): %w", i+1, command, err)
		}

		log.Printf("Command %d completed successfully", i+1)
	}

	return nil
}

// executeCommand executes a single shell command with timeout
func (e *ShellCommandExecutor) executeCommand(command, workingDir string) error {
	ctx, cancel := context.WithTimeout(context.Background(), e.timeout)
	defer cancel()

	// Use shell to execute the command so we support pipes, redirects, etc.
	cmd := exec.CommandContext(ctx, "sh", "-c", command)
	cmd.Dir = workingDir

	output, err := cmd.CombinedOutput()
	if err != nil {
		if ctx.Err() == context.DeadlineExceeded {
			return fmt.Errorf("command timed out after %v", e.timeout)
		}
		return fmt.Errorf("command failed with exit code %d: %s", cmd.ProcessState.ExitCode(), string(output))
	}

	// Log command output if there is any
	if len(output) > 0 {
		log.Printf("Command output: %s", string(output))
	}

	return nil
}