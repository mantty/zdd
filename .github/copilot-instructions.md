# Project Overview

This project contains the zdd (zero downtime deployments) CLI tool.
zdd allows users to create and run SQL migrations in multiple steps to ensure migrations apply without causing downtime. It is inspired by the expand-migration-contract flow and is built using go.

## ZDD functionality

- A user creates a new empty deployment with `zdd create`
- A deployment is composed of multiple sql files and commands to be executed
- The latest deployment is applied with `zdd deploy`
- Deployments apply by running commands, then executing the sql file(s) for that step
- The steps in order are expand, migrate, contract
- ZDD keeps track of applied deployments inside the `zdd_deployments` schema

## Folder structure

- `/hack` contains any scripts and helpers useful for local dev
- `/cmd/` contains the executable entrypoint
- `/testdata` contains non-code assets used during testing (such as fixture values)
- `/*` contains go files in the zdd package, other subdirectories contain functionality that has been split into separate go packages

## Libraries and frameworks

- github.com/urfave/cli/v3 is used for the command, flags, args, and input config parsing
- github.com/testcontainers/testcontainers-go is used to create any docker containers (e.g. postgres) required to run tests

## Coding standards

- Always seek to make the minimal change that satisfies the request whilst maintaining wider system functionality, do not unnecessarily refactor other code
- Always consider YAGNI + SOLID + KISS + DRY principles when designing, reviewing, or adding new code.
- Simple is better than complex
- Flat is better than nested
- Explicit is better than implicit
