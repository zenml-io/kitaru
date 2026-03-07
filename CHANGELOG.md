# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/),
and this project adheres to [Semantic Versioning](https://semver.org/).

## [Unreleased]

### Added
- Global log-store configuration with `kitaru log-store set/show/reset`
- Persisted Kitaru user config (`kitaru.yaml`) for log-store override state
- Environment override support for runtime log-store resolution

### Changed
- Updated README, CLAUDE guide, and docs landing page to reflect implemented `kitaru.log()` and log-store CLI behavior

## [0.1.0] - 2026-03-06

### Added
- Initial project scaffolding with uv, ruff, ty, and CI
- CLI with cyclopts (`kitaru --version`, `kitaru --help`)
- Justfile for common development commands
- Link checking with lychee
- Typo checking with typos
