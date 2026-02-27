# agenttrace

[![CI](https://github.com/npow/agenttrace/actions/workflows/ci.yml/badge.svg)](https://github.com/npow/agenttrace/actions/workflows/ci.yml)
[![PyPI](https://img.shields.io/pypi/v/agenttrace)](https://pypi.org/project/agenttrace/)
[![License: Apache-2.0](https://img.shields.io/badge/License-Apache--2.0-blue.svg)](LICENSE)
[![Python 3.11+](https://img.shields.io/badge/python-3.11+-blue.svg)](https://www.python.org/downloads/)

See exactly what your AI coding agent did — every tool call, error, and decision, ingested in real time.

## The problem

Claude Code and similar agents generate rich session logs, but those logs are raw JSONL blobs buried in `~/.claude/projects/`. There's no way to query your sessions, spot error patterns, or understand where the agent stumbled. You're flying blind on what your agent actually did.

## Quick start

```bash
pip install agenttrace
agenttrace ingest        # one-shot: parse all sessions into SQLite
agenttrace start         # daemon: watch for new sessions in real time
```

Your sessions are now in `~/.agenttrace/data.sqlite` — query with any SQLite client.

## Install

```bash
pip install agenttrace
```

From source:

```bash
git clone https://github.com/npow/agenttrace.git
cd agenttrace
make dev-install
source .venv/bin/activate
```

## Usage

### One-shot ingestion

Parse all existing Claude Code sessions:

```bash
agenttrace ingest
# Done. 42/42 files ingested, 18,302 raw entries, 5,841 progress entries
# (0 skipped, 0 failed). DB totals: 18302 entries, 127 sessions, 12 projects.
```

### Real-time daemon

Watch `~/.claude/projects/` and ingest new entries as sessions run:

```bash
agenttrace start
# Watching ~/.claude/projects → ~/.agenttrace/data.sqlite
```

### Re-ingest from scratch

```bash
agenttrace ingest --force
```

### Custom paths

```bash
agenttrace start \
  --db /path/to/my.sqlite \
  --sources-dir /path/to/sessions
```

## How it works

`agenttrace` watches `~/.claude/projects/` for JSONL session files using watchdog. When a file changes, it runs an incremental parse: only new lines are read, tool calls and errors are classified, and everything is written to SQLite with WAL mode for concurrent access. A 30-second debounce prevents redundant ingestion when many files change at once.

The `@agenttrace/viewer` npm package provides a React component library for building session visualizers. The `views/` directory contains example view plugins (like `process-monitor`) that implement the viewer API.

## Development

```bash
git clone https://github.com/npow/agenttrace.git
cd agenttrace
make test
```

## License

Apache-2.0 — see [LICENSE](LICENSE).
