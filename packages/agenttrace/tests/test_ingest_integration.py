"""Integration tests for ingest_file() — full pipeline from JSONL → SQLite."""

import importlib
import json
import os
import sqlite3
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_db(db_path: Path):
    """Set env, reload modules, return (conn, db_mod, ingest_mod)."""
    os.environ["CLAUDE_RETRO_DB"] = str(db_path)
    import sessionlog.config as cfg
    import sessionlog.db as db_mod
    import sessionlog.ingest as ingest_mod

    importlib.reload(cfg)
    importlib.reload(db_mod)
    importlib.reload(ingest_mod)

    conn = db_mod.get_writer()
    return conn, db_mod, ingest_mod


def _write_jsonl(path: Path, records: list[dict]) -> None:
    with open(path, "w") as f:
        for rec in records:
            f.write(json.dumps(rec) + "\n")


# ---------------------------------------------------------------------------
# Sample records
# ---------------------------------------------------------------------------

RAW_ASSISTANT = {
    "type": "assistant",
    "uuid": "raw-asst-1",
    "sessionId": "session-int-1",
    "timestamp": "2024-01-15T10:00:00Z",
    "parentUuid": None,
    "isSidechain": False,
    "gitBranch": "main",
    "cwd": "/project",
    "message": {
        "model": "claude-opus-4-6",
        "content": [
            {"type": "tool_use", "id": "tu1", "name": "Read", "input": {"file_path": "/foo.py"}},
        ],
        "usage": {"input_tokens": 200, "output_tokens": 30},
    },
}

RAW_USER_TOOL_RESULT = {
    "type": "user",
    "uuid": "raw-user-1",
    "sessionId": "session-int-1",
    "timestamp": "2024-01-15T10:00:01Z",
    "parentUuid": "raw-asst-1",
    "isSidechain": False,
    "message": {
        "content": [
            {
                "type": "tool_result",
                "tool_use_id": "tu1",
                "content": "file content here",
                "is_error": False,
            }
        ]
    },
}

PROGRESS_AGENT = {
    "type": "progress",
    "uuid": "prog-1",
    "sessionId": "session-int-1",
    "parentUuid": "parent-task-id",
    "timestamp": "2024-01-15T10:00:02Z",
    "data": {
        "type": "agent_progress",
        "message": {
            "type": "assistant",
            "message": {
                "role": "assistant",
                "content": [
                    {"type": "tool_use", "id": "sub-bash-1", "name": "Bash", "input": {"command": "ls"}},
                ],
            },
        },
    },
}

PROGRESS_BASH = {
    "type": "progress",
    "uuid": "bash-prog-1",
    "sessionId": "session-int-1",
    "parentUuid": "bash-tool-id",
    "timestamp": "2024-01-15T10:00:03Z",
    "data": {
        "type": "bash_progress",
        "output": "running...",
    },
}

PROGRESS_MCP = {
    "type": "progress",
    "uuid": "mcp-prog-1",
    "sessionId": "session-int-1",
    "timestamp": "2024-01-15T10:00:04Z",
    "data": {
        "type": "mcp_progress",
        "toolUseId": "mcp-tool-id",
    },
}

SYSTEM_TURN = {
    "type": "system",
    "subtype": "turn_duration",
    "uuid": "sys-1",
    "sessionId": "session-int-1",
    "timestamp": "2024-01-15T10:00:05Z",
    "durationMs": 5000,
}


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestIngestFileCounts:
    def test_ingest_file_returns_correct_raw_and_progress_counts(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [
            RAW_ASSISTANT,
            RAW_USER_TOOL_RESULT,
            SYSTEM_TURN,
            PROGRESS_AGENT,
            PROGRESS_BASH,
            PROGRESS_MCP,   # should NOT be counted
        ])

        raw_count, progress_count = ingest_mod.ingest_file(jsonl, "proj", conn)

        # 3 raw records: assistant, user, system
        assert raw_count == 3
        # 2 progress records: agent_progress + bash_progress (mcp_progress excluded)
        assert progress_count == 2


class TestProgressEntriesTable:
    def test_progress_entries_stored_after_ingest(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [PROGRESS_AGENT, PROGRESS_BASH])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        rows = conn.execute("SELECT entry_id FROM progress_entries ORDER BY entry_id").fetchall()
        entry_ids = {r[0] for r in rows}
        assert "prog-1" in entry_ids
        assert "bash-prog-1" in entry_ids

    def test_agent_progress_stored_with_correct_tool_name_and_has_result(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [PROGRESS_AGENT])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        row = conn.execute(
            "SELECT progress_type, tool_name, has_result, result_error FROM progress_entries WHERE entry_id = ?",
            ["prog-1"],
        ).fetchone()
        assert row is not None
        progress_type, tool_name, has_result, result_error = row
        assert progress_type == "agent_progress"
        assert tool_name == "Bash"
        assert has_result == 0   # assistant message with tool_use; result comes in a separate user message
        assert result_error == 0

    def test_bash_progress_stored_with_correct_progress_type_and_no_tool_name(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [PROGRESS_BASH])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        row = conn.execute(
            "SELECT progress_type, tool_name, has_result FROM progress_entries WHERE entry_id = ?",
            ["bash-prog-1"],
        ).fetchone()
        assert row is not None
        progress_type, tool_name, has_result = row
        assert progress_type == "bash_progress"
        assert tool_name is None
        assert has_result == 0

    def test_mcp_progress_not_stored(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [PROGRESS_MCP])

        raw_count, progress_count = ingest_mod.ingest_file(jsonl, "proj", conn)

        assert progress_count == 0
        row = conn.execute(
            "SELECT entry_id FROM progress_entries WHERE entry_id = ?",
            ["mcp-prog-1"],
        ).fetchone()
        assert row is None


class TestRawEntriesTable:
    def test_raw_entries_stored_correctly(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [RAW_ASSISTANT, RAW_USER_TOOL_RESULT])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        rows = conn.execute("SELECT entry_id FROM raw_entries ORDER BY entry_id").fetchall()
        entry_ids = {r[0] for r in rows}
        assert "raw-asst-1" in entry_ids
        assert "raw-user-1" in entry_ids

    def test_assistant_entry_has_tool_names(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [RAW_ASSISTANT])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        row = conn.execute(
            "SELECT tool_names FROM raw_entries WHERE entry_id = ?",
            ["raw-asst-1"],
        ).fetchone()
        assert row is not None
        tool_names = json.loads(row[0])
        assert "Read" in tool_names

    def test_user_tool_result_flagged_correctly(self, tmp_path):
        db_path = tmp_path / "test.sqlite"
        conn, _, ingest_mod = _make_db(db_path)

        jsonl = tmp_path / "session.jsonl"
        _write_jsonl(jsonl, [RAW_USER_TOOL_RESULT])

        ingest_mod.ingest_file(jsonl, "proj", conn)

        row = conn.execute(
            "SELECT is_tool_result, tool_result_error FROM raw_entries WHERE entry_id = ?",
            ["raw-user-1"],
        ).fetchone()
        assert row is not None
        is_tool_result, tool_result_error = row
        assert is_tool_result == 1
        assert tool_result_error == 0
