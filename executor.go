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

// ExecuteCommand executes a single shell command
func (e *ShellCommandExecutor) ExecuteCommand(command string, workingDir string) error {
	if strings.TrimSpace(command) == "" {
		return nil
	}

	log.Printf("Executing command in directory: %s", workingDir)
	log.Printf("Running command: %s", command)

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

	log.Printf("Command completed successfully")
	return nil
}
