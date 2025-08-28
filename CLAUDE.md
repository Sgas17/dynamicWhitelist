# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is a dynamicWhitelist project - a standalone Docker container system that creates dynamic whitelists based on pre-defined criteria. The whitelists are published on Redis + NATS for other services to consume.

## Current State

This appears to be an early-stage project with minimal codebase:
- Contains only a README.md with basic project description
- Has GitHub Actions workflow configured for Claude Code integration
- No source code files, package configuration, or Docker files present yet

## GitHub Integration

The repository is configured with Claude Code GitHub Actions integration via `.github/workflows/claude.yml`. Claude can be triggered by:
- Comments containing `@claude` on issues or pull requests
- Issue creation with `@claude` in title or body
- Pull request review comments containing `@claude`

The workflow has permissions for:
- Repository content modification
- Pull request and issue management
- Reading CI results

## Development Commands

Since this is an early-stage project with no package.json, Dockerfile, or other configuration files, standard build/test/lint commands are not yet established. These will need to be defined as the project develops.