"""Flask REST API."""

import json
import sys
from pathlib import Path

from flask import Flask, jsonify, request, send_from_directory, Response

from agenttrace.config import CLAUDE_PROJECTS_DIR
from agenttrace.db import get_conn
from retro.version import get_version_info
from retro.export import generate_export_html

if getattr(sys, "frozen", False):
    _static = str(Path(sys._MEIPASS) / "static")
else:
    _static = str(Path(__file__).parent / "static")

app = Flask(__name__, static_folder=_static)

# Set by app.py / __main__.py so /api/status can read worker state
_worker = None


def set_worker(worker):
    global _worker
    _worker = worker


@app.route("/api/status")
def api_status():
    if _worker is None:
        return jsonify({"state": "idle", "step": "", "ready": True})
    return jsonify(_worker.status)


def _row_to_dict(row, columns):
    return {col: _serialize(val) for col, val in zip(columns, row)}


def _serialize(val):
    if hasattr(val, "isoformat"):
        return val.isoformat()
    return val


@app.route("/")
def index():
    return send_from_directory(app.static_folder, "index.html")


@app.route("/api/version")
def api_version():
    return jsonify(get_version_info())


@app.route("/api/export")
def api_export():
    """Export verdict and prescriptions as standalone HTML."""
    from datetime import datetime

    html = generate_export_html()
    filename = f"claude-retro-{datetime.now().strftime('%Y%m%d')}.html"

    return Response(
        html,
        mimetype="text/html",
        headers={"Content-Disposition": f"attachment; filename={filename}"},
    )


@app.route("/api/overview")
def api_overview():
    conn = get_conn()

    _filter = """
        WHERE turn_count >= 1
          AND first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    stats = conn.execute(f"""
        SELECT
            COUNT(*) as total_sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as total_hours,
            COUNT(DISTINCT project_name) as total_projects,
            AVG(turn_count) as avg_turns,
            SUM(user_prompt_count + assistant_msg_count) as total_messages,
            COUNT(DISTINCT DATE(started_at)) as active_days,
            SUM(s.tool_use_count) as total_tool_calls,
            COALESCE(SUM(f.total_input_tokens), 0) as total_input_tokens,
            COALESCE(SUM(f.total_output_tokens), 0) as total_output_tokens
        FROM sessions s
        LEFT JOIN session_features f ON s.session_id = f.session_id
        {_filter}
    """).fetchone()

    # Median and p90 of messages per session
    msg_counts = conn.execute(f"""
        SELECT user_prompt_count + assistant_msg_count as msgs
        FROM sessions
        {_filter}
        ORDER BY msgs
    """).fetchall()
    msg_list = [r[0] for r in msg_counts if r[0] is not None]
    if msg_list:
        median_msgs = msg_list[len(msg_list) // 2]
        p90_msgs = msg_list[int(len(msg_list) * 0.9)]
        avg_msgs = round(sum(msg_list) / len(msg_list), 1)
    else:
        median_msgs = p90_msgs = avg_msgs = 0

    # Top project concentration
    top_proj = conn.execute(f"""
        SELECT project_name, COUNT(*) as cnt
        FROM sessions
        {_filter}
        GROUP BY project_name
        ORDER BY cnt DESC
        LIMIT 1
    """).fetchone()

    trajectory_dist = conn.execute(f"""
        SELECT trajectory, COUNT(*) as count
        FROM sessions
        {_filter}
        GROUP BY trajectory
        ORDER BY count DESC
    """).fetchall()

    cursor = conn.execute("""
        SELECT * FROM baselines ORDER BY window_size
    """)
    baselines = cursor.fetchall()
    baseline_cols = [d[0] for d in cursor.description]

    total_sessions = stats[0] or 0

    return jsonify(
        {
            "total_sessions": total_sessions,
            "avg_convergence": round(stats[1] or 0, 3),
            "avg_drift": round(stats[2] or 0, 3),
            "avg_thrash": round(stats[3] or 0, 3),
            "total_hours": round(stats[4] or 0, 1),
            "total_projects": stats[5],
            "avg_turns": round(stats[6] or 0, 1),
            "total_messages": stats[7] or 0,
            "active_days": stats[8] or 0,
            "msgs_per_session_avg": avg_msgs,
            "msgs_per_session_median": median_msgs,
            "msgs_per_session_p90": p90_msgs,
            "top_project": top_proj[0] if top_proj else None,
            "top_project_pct": round(top_proj[1] / total_sessions, 2) if top_proj and total_sessions else 0,
            "trajectory_distribution": {t: c for t, c in trajectory_dist},
            "baselines": [_row_to_dict(b, baseline_cols) for b in baselines],
            "total_tool_calls": int(stats[9] or 0),
            "total_input_tokens": int(stats[10] or 0),
            "total_output_tokens": int(stats[11] or 0),
            # Estimated cost: Sonnet 3.5/3.7 pricing ($3/MTok in, $15/MTok out)
            "estimated_cost_usd": round(
                (stats[10] or 0) / 1_000_000 * 3.0
                + (stats[11] or 0) / 1_000_000 * 15.0,
                2,
            ),
        }
    )


@app.route("/api/sessions")
def api_sessions():
    conn = get_conn()
    project = request.args.get("project")
    intent = request.args.get("intent")
    trajectory = request.args.get("trajectory")
    search = request.args.get("search")
    limit = int(request.args.get("limit", 50))
    offset = int(request.args.get("offset", 0))
    sort = request.args.get("sort", "started_at DESC")

    # Whitelist sort columns
    allowed_sorts = {
        "started_at": "s.started_at",
        "convergence": "s.convergence_score",
        "drift": "s.drift_score",
        "thrash": "s.thrash_score",
        "duration": "s.duration_seconds",
        "turns": "s.turn_count",
        "misalignments": "COALESCE(j.misalignment_count, 0)",
        "productivity": "COALESCE(j.productivity_ratio, 0)",
    }
    sort_parts = sort.split()
    sort_col = allowed_sorts.get(sort_parts[0], "s.started_at")
    sort_dir = (
        "DESC" if len(sort_parts) < 2 or sort_parts[1].upper() == "DESC" else "ASC"
    )

    conditions = ["s.turn_count >= 1", "s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'"]
    params = []

    if project:
        conditions.append("s.project_name = ?")
        params.append(project)
    if intent:
        conditions.append("s.intent = ?")
        params.append(intent)
    if trajectory:
        conditions.append("s.trajectory = ?")
        params.append(trajectory)
    if search:
        conditions.append("(s.first_prompt LIKE ? OR s.session_id LIKE ?)")
        params.extend([f"%{search}%", f"%{search}%"])

    where = "WHERE " + " AND ".join(conditions)

    total = conn.execute(f"SELECT COUNT(*) FROM sessions s {where}", params).fetchone()[
        0
    ]

    rows = conn.execute(
        f"""
        SELECT s.session_id, s.project_name, s.started_at, s.ended_at, s.duration_seconds,
               s.user_prompt_count, s.assistant_msg_count, s.tool_use_count, s.tool_error_count,
               s.turn_count, s.first_prompt, s.intent, s.trajectory,
               s.convergence_score, s.drift_score, s.thrash_score,
               j.outcome, j.misalignment_count, j.productivity_ratio
        FROM sessions s
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        {where}
        ORDER BY {sort_col} {sort_dir}
        LIMIT ? OFFSET ?
    """,
        params + [limit, offset],
    ).fetchall()

    cols = [
        "session_id",
        "project_name",
        "started_at",
        "ended_at",
        "duration_seconds",
        "user_prompt_count",
        "assistant_msg_count",
        "tool_use_count",
        "tool_error_count",
        "turn_count",
        "first_prompt",
        "intent",
        "trajectory",
        "convergence_score",
        "drift_score",
        "thrash_score",
        "judgment_outcome",
        "misalignment_count",
        "productivity_ratio",
    ]

    return jsonify(
        {
            "total": total,
            "sessions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/sessions/<session_id>")
def api_session_detail(session_id):
    conn = get_conn()

    cursor = conn.execute(
        """
        SELECT * FROM sessions WHERE session_id = ?
    """,
        [session_id],
    )
    session = cursor.fetchone()

    if not session:
        return jsonify({"error": "Session not found"}), 404

    session_cols = [d[0] for d in cursor.description]

    cursor2 = conn.execute(
        """
        SELECT * FROM session_features WHERE session_id = ?
    """,
        [session_id],
    )
    features = cursor2.fetchone()
    feature_cols = [d[0] for d in cursor2.description] if features else []

    tools = conn.execute(
        """
        SELECT tool_name, use_count, error_count
        FROM session_tool_usage WHERE session_id = ?
        ORDER BY use_count DESC
    """,
        [session_id],
    ).fetchall()

    cursor3 = conn.execute(
        """
        SELECT * FROM session_judgments WHERE session_id = ?
    """,
        [session_id],
    )
    judgment = cursor3.fetchone()
    judgment_cols = [d[0] for d in cursor3.description] if judgment else []

    result = {
        "session": _row_to_dict(session, session_cols),
        "features": _row_to_dict(features, feature_cols) if features else {},
        "tools": [
            {"tool_name": t[0], "use_count": t[1], "error_count": t[2]} for t in tools
        ],
    }
    if judgment:
        jd = _row_to_dict(judgment, judgment_cols)
        # Parse JSON string fields for the frontend
        for field in (
            "prompt_missing",
            "underspecified_parts",
            "misalignments",
            "corrections",
            "waste_breakdown",
        ):
            if jd.get(field) and isinstance(jd[field], str):
                try:
                    jd[field] = json.loads(jd[field])
                except (json.JSONDecodeError, ValueError):
                    pass
        result["judgment"] = jd
        # Include narrative fields at top level for easy access
        result["narrative"] = {
            "narrative": jd.get("narrative"),
            "what_worked": jd.get("what_worked"),
            "what_failed": jd.get("what_failed"),
            "user_quote": jd.get("user_quote"),
            "claude_md_suggestion": jd.get("claude_md_suggestion"),
            "claude_md_rationale": jd.get("claude_md_rationale"),
        }
    else:
        result["judgment"] = None
        result["narrative"] = None

    return jsonify(result)


@app.route("/api/sessions/<session_id>/timeline")
def api_session_timeline(session_id):
    conn = get_conn()

    full = request.args.get("full", "0") == "1"
    text_col = "user_text" if full else "SUBSTR(user_text, 1, 200)"
    content_col = "text_content" if full else "SUBSTR(text_content, 1, 200)"

    entries = conn.execute(
        f"""
        SELECT entry_id, entry_type, timestamp_utc, user_text_length,
               text_length, tool_names, is_tool_result, tool_result_error,
               system_subtype, duration_ms,
               CASE WHEN user_text_length > 0 THEN {text_col} ELSE {content_col} END as preview,
               CASE WHEN user_text_length > 0 THEN {text_col} ELSE NULL END as user_text
        FROM raw_entries
        WHERE session_id = ? AND NOT is_sidechain
        ORDER BY timestamp_utc
    """,
        [session_id],
    ).fetchall()

    cols = [
        "entry_id",
        "entry_type",
        "timestamp_utc",
        "user_text_length",
        "text_length",
        "tool_names",
        "is_tool_result",
        "tool_result_error",
        "system_subtype",
        "duration_ms",
        "preview",
        "user_text",
    ]

    return jsonify(
        {
            "timeline": [_row_to_dict(e, cols) for e in entries],
        }
    )


@app.route("/api/sessions/<session_id>/rich-timeline")
def api_session_rich_timeline(session_id):
    """Read JSONL directly to return full tool inputs + result content."""
    conn = get_conn()

    row = conn.execute(
        "SELECT project_name FROM sessions WHERE session_id = ?", [session_id]
    ).fetchone()
    if not row:
        return jsonify({"error": "Session not found", "timeline": []}), 404

    project_name = row[0]
    jsonl_path = CLAUDE_PROJECTS_DIR / project_name / f"{session_id}.jsonl"

    if not jsonl_path.exists():
        return jsonify({"error": "JSONL not found", "timeline": []}), 404

    MAX = 400
    turns = []

    with open(jsonl_path, "r", errors="replace") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                d = json.loads(line)
            except json.JSONDecodeError:
                continue

            entry_type = d.get("type")
            if entry_type not in ("user", "assistant", "system"):
                continue
            if d.get("isSidechain"):
                continue

            msg = d.get("message", {})
            content = msg.get("content", "")

            turn = {
                "type": entry_type,
                "timestamp": d.get("timestamp", ""),
                "text": "",
                "tools": [],
                "is_tool_result": False,
                "is_error": False,
                "tool_id": None,
                "result_preview": None,
                "system_subtype": d.get("subtype"),
                "duration_ms": d.get("durationMs", 0),
            }

            if isinstance(content, str):
                turn["text"] = content[:MAX]
            elif isinstance(content, list):
                text_parts = []
                for block in content:
                    btype = block.get("type", "")
                    if btype == "text":
                        text_parts.append(block.get("text", ""))
                    elif btype == "tool_use":
                        inp = block.get("input", {})
                        inp_str = json.dumps(inp, ensure_ascii=False)
                        turn["tools"].append({
                            "name": block.get("name", ""),
                            "id": block.get("id", ""),
                            "input_preview": inp_str[:MAX],
                        })
                    elif btype == "tool_result":
                        turn["is_tool_result"] = True
                        turn["is_error"] = bool(block.get("is_error", False))
                        turn["tool_id"] = block.get("tool_use_id", "")
                        rc = block.get("content", "")
                        if isinstance(rc, list):
                            rc = " ".join(
                                b.get("text", "") for b in rc if b.get("type") == "text"
                            )
                        turn["result_preview"] = str(rc)[:MAX] if rc else None
                if text_parts:
                    turn["text"] = "\n".join(text_parts)[:MAX]

            turns.append(turn)

    return jsonify({"timeline": turns})


@app.route("/api/intents")
def api_intents():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            intent,
            COUNT(*) as count,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            AVG(duration_seconds) as avg_duration,
            AVG(turn_count) as avg_turns
        FROM sessions
        GROUP BY intent
        ORDER BY count DESC
    """).fetchall()

    cols = [
        "intent",
        "count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "avg_duration",
        "avg_turns",
    ]

    return jsonify(
        {
            "intents": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/trends")
def api_trends():
    conn = get_conn()
    days = int(request.args.get("days", 30))

    rows = conn.execute(
        """
        SELECT
            DATE(started_at) as day,
            COUNT(*) as sessions,
            AVG(convergence_score) as avg_convergence,
            AVG(drift_score) as avg_drift,
            AVG(thrash_score) as avg_thrash,
            SUM(duration_seconds) / 3600.0 as hours
        FROM sessions
        WHERE started_at >= DATE('now', '-? days')
        GROUP BY DATE(started_at)
        ORDER BY day
    """.replace("?", str(int(days)))
    ).fetchall()

    cols = ["day", "sessions", "avg_convergence", "avg_drift", "avg_thrash", "hours"]

    return jsonify(
        {
            "trends": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/search")
def api_search():
    """Full-text search across all messages using FTS5."""
    conn = get_conn()
    q = request.args.get("q", "").strip()
    limit = min(int(request.args.get("limit", 30)), 100)
    project = request.args.get("project")

    if not q or len(q) < 2:
        return jsonify({"results": [], "query": q})

    # Escape FTS5 special characters and wrap in quotes for phrase matching
    fts_query = q.replace('"', '""')
    if " " in fts_query:
        fts_query = f'"{fts_query}"'

    try:
        params = [fts_query]
        project_filter = ""
        if project:
            project_filter = "AND s.project_name = ?"
            params.append(project)
        params.append(limit)

        rows = conn.execute(f"""
            SELECT
                messages_fts.session_id,
                messages_fts.entry_type,
                s.project_name,
                s.first_prompt,
                s.started_at,
                snippet(messages_fts, 0, '<mark>', '</mark>', '...', 40) as snippet,
                s.started_at as timestamp_utc
            FROM messages_fts
            JOIN sessions s ON messages_fts.session_id = s.session_id
            WHERE messages_fts MATCH ?
              {project_filter}
            ORDER BY rank
            LIMIT ?
        """, params).fetchall()
    except Exception:
        # FTS query failed — fall back to LIKE search
        like_q = f"%{q}%"
        params = [like_q, like_q]
        project_filter = ""
        if project:
            project_filter = "AND s.project_name = ?"
            params.append(project)
        params.append(limit)

        rows = conn.execute(f"""
            SELECT
                r.session_id,
                r.entry_type,
                s.project_name,
                s.first_prompt,
                s.started_at,
                SUBSTR(COALESCE(r.user_text, r.text_content, ''), 1, 200) as snippet,
                r.timestamp_utc
            FROM raw_entries r
            JOIN sessions s ON r.session_id = s.session_id
            WHERE (r.user_text LIKE ? OR r.text_content LIKE ?)
              {project_filter}
            ORDER BY r.timestamp_utc DESC
            LIMIT ?
        """, params).fetchall()

    results = []
    seen_sessions = set()
    for row in rows:
        sid = row[0]
        # Deduplicate by session (show max 2 results per session)
        count = sum(1 for r in results if r["session_id"] == sid)
        if count >= 2:
            continue
        results.append({
            "session_id": sid,
            "entry_type": row[1],
            "project": row[2],
            "first_prompt": (row[3] or "")[:80],
            "started_at": _serialize(row[4]),
            "snippet": row[5],
            "timestamp": _serialize(row[6]),
        })

    return jsonify({"results": results, "query": q})


@app.route("/api/actions")
def api_actions():
    from retro.prescriptions import generate_actions

    actions = generate_actions()
    return jsonify({"actions": actions})


@app.route("/api/prescriptions")
def api_prescriptions():
    conn = get_conn()

    rows = conn.execute("""
        SELECT id, category, title, description, evidence, confidence, dismissed, created_at
        FROM prescriptions
        WHERE dismissed = FALSE
        ORDER BY confidence DESC
    """).fetchall()

    cols = [
        "id",
        "category",
        "title",
        "description",
        "evidence",
        "confidence",
        "dismissed",
        "created_at",
    ]

    return jsonify(
        {
            "prescriptions": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/prescriptions/<int:pid>/dismiss", methods=["POST"])
def api_dismiss_prescription(pid):
    conn = get_conn()
    conn.execute("UPDATE prescriptions SET dismissed = TRUE WHERE id = ?", [pid])
    return jsonify({"ok": True})


@app.route("/api/tools")
def api_tools():
    conn = get_conn()

    rows = conn.execute("""
        SELECT tool_name, SUM(use_count) as total_uses, SUM(error_count) as total_errors
        FROM session_tool_usage
        GROUP BY tool_name
        ORDER BY total_uses DESC
    """).fetchall()

    return jsonify(
        {
            "tools": [
                {"tool_name": r[0], "total_uses": r[1], "total_errors": r[2]}
                for r in rows
            ],
        }
    )


@app.route("/api/projects")
def api_projects():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            s.project_name,
            COUNT(*) as session_count,
            AVG(s.convergence_score) as avg_convergence,
            AVG(s.drift_score) as avg_drift,
            AVG(s.thrash_score) as avg_thrash,
            SUM(s.duration_seconds) / 3600.0 as total_hours,
            MAX(s.started_at) as last_active,
            SUM(s.tool_error_count) as total_errors,
            COALESCE(SUM(f.total_input_tokens), 0) as total_input_tokens,
            COALESCE(SUM(f.total_output_tokens), 0) as total_output_tokens,
            AVG(j.productivity_ratio) as avg_productivity,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1.0 ELSE 0.0 END)
                / NULLIF(SUM(CASE WHEN j.outcome IS NOT NULL THEN 1 ELSE 0 END), 0) as completion_rate,
            AVG(j.misalignment_count) as avg_misalignments
        FROM sessions s
        LEFT JOIN session_features f ON s.session_id = f.session_id
        LEFT JOIN session_judgments j ON s.session_id = j.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%%'
        GROUP BY s.project_name
        ORDER BY session_count DESC
    """).fetchall()

    cols = [
        "project_name",
        "session_count",
        "avg_convergence",
        "avg_drift",
        "avg_thrash",
        "total_hours",
        "last_active",
        "total_errors",
        "total_input_tokens",
        "total_output_tokens",
        "avg_productivity",
        "completion_rate",
        "avg_misalignments",
    ]

    return jsonify(
        {
            "projects": [_row_to_dict(r, cols) for r in rows],
        }
    )


@app.route("/api/refresh", methods=["POST"])
def api_refresh():
    """Trigger a full refresh (ingest + LLM judge) in the background.

    Non-blocking — returns immediately. Poll /api/status for progress.
    Accepts optional JSON body: {"concurrency": 12}
    """
    if _worker is None:
        return jsonify({"error": "No background worker available"}), 500

    if _worker.is_busy:
        # Queue it — the worker will pick it up after the current run finishes
        _worker.request_refresh(
            concurrency=max(
                1,
                min(
                    32,
                    int((request.get_json(silent=True) or {}).get("concurrency", 12)),
                ),
            )
        )
        return jsonify({"ok": True, "queued": True, "concurrency": 12})

    body = request.get_json(silent=True) or {}
    concurrency = body.get("concurrency", 12)
    concurrency = max(1, min(32, int(concurrency)))

    _worker.request_refresh(concurrency=concurrency)
    return jsonify({"ok": True, "concurrency": concurrency})


@app.route("/api/judgments/stats")
def api_judgment_stats():
    conn = get_conn()

    # Only count judgments for meaningful sessions (same filter as overview/session list)
    _jfilter = """
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    total = conn.execute(f"SELECT COUNT(*) {_jfilter}").fetchone()[0]
    if total == 0:
        return jsonify({"total_judged": 0})

    outcome_dist = conn.execute(f"""
        SELECT j.outcome, COUNT(*) as count
        {_jfilter}
        GROUP BY j.outcome ORDER BY count DESC
    """).fetchall()

    avgs = conn.execute(f"""
        SELECT
            AVG(j.prompt_clarity) as avg_clarity,
            AVG(j.prompt_completeness) as avg_completeness,
            AVG(j.productivity_ratio) as avg_productivity,
            AVG(j.misalignment_count) as avg_misalignments,
            SUM(CASE WHEN j.misalignment_count > 0 THEN 1 ELSE 0 END) as sessions_with_misalignment
        {_jfilter}
    """).fetchone()

    return jsonify(
        {
            "total_judged": total,
            "outcome_distribution": {r[0]: r[1] for r in outcome_dist},
            "avg_clarity": round(avgs[0] or 0, 3),
            "avg_completeness": round(avgs[1] or 0, 3),
            "avg_productivity": round(avgs[2] or 0, 3),
            "avg_misalignments": round(avgs[3] or 0, 2),
            "misalignment_rate": round((avgs[4] or 0) / total, 3) if total else 0,
        }
    )


@app.route("/api/patterns")
def api_patterns():
    conn = get_conn()

    # --- Prompt gap clustering ---
    gap_rows = conn.execute("""
        SELECT prompt_missing FROM session_judgments
        WHERE prompt_missing IS NOT NULL AND prompt_missing != '[]'
    """).fetchall()

    GAP_CATEGORIES = {
        "context": [
            "repo",
            "codebase",
            "file",
            "directory",
            "structure",
            "existing",
            "path",
            "folder",
            "project",
        ],
        "requirements": [
            "expected",
            "behavior",
            "output",
            "format",
            "specific",
            "requirement",
            "result",
            "goal",
        ],
        "constraints": [
            "environment",
            "version",
            "dependency",
            "platform",
            "setup",
            "config",
            "os",
            "runtime",
        ],
        "error_details": [
            "error",
            "message",
            "stack",
            "trace",
            "log",
            "exception",
            "warning",
            "failure",
        ],
        "scope": [
            "which",
            "where",
            "boundary",
            "limit",
            "priority",
            "scope",
            "range",
            "subset",
        ],
    }

    gap_counts = {cat: {"count": 0, "examples": []} for cat in GAP_CATEGORIES}
    total_gap_items = 0
    for (raw,) in gap_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                text = str(item).lower()
                total_gap_items += 1
                matched = False
                for cat, keywords in GAP_CATEGORIES.items():
                    if any(kw in text for kw in keywords):
                        gap_counts[cat]["count"] += 1
                        if len(gap_counts[cat]["examples"]) < 3:
                            gap_counts[cat]["examples"].append(str(item))
                        matched = True
                        break
                if not matched:
                    # Assign to "other" implicitly by not counting
                    pass
        except (json.JSONDecodeError, TypeError):
            continue

    prompt_gaps = sorted(
        [
            {
                "category": cat,
                "examples": info["examples"],
                "count": info["count"],
                "pct": round(info["count"] / total_gap_items, 2)
                if total_gap_items
                else 0,
            }
            for cat, info in gap_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Misalignment theme clustering ---
    mis_rows = conn.execute("""
        SELECT misalignments FROM session_judgments
        WHERE misalignments IS NOT NULL AND misalignments != '[]'
    """).fetchall()

    THEME_KEYWORDS = {
        "tool_overuse": ["tool", "unnecessary", "redundant", "excessive", "repeated"],
        "wrong_approach": [
            "wrong",
            "incorrect",
            "different approach",
            "should have",
            "instead of",
        ],
        "scope_drift": [
            "scope",
            "beyond",
            "unrelated",
            "off-topic",
            "tangent",
            "extra",
        ],
        "format_mismatch": ["format", "style", "convention", "naming", "pattern"],
        "misunderstood_intent": [
            "misunderstood",
            "misinterpret",
            "not what",
            "didn't ask",
            "intent",
        ],
        "ignored_feedback": [
            "ignored",
            "repeated",
            "already said",
            "told you",
            "again",
        ],
    }

    theme_counts = {
        t: {"count": 0, "example": "", "sessions": []} for t in THEME_KEYWORDS
    }
    for (raw,) in mis_rows:
        try:
            items = json.loads(raw) if isinstance(raw, str) else raw
            for item in items:
                desc = (
                    item.get("description", str(item))
                    if isinstance(item, dict)
                    else str(item)
                ).lower()
                for theme, keywords in THEME_KEYWORDS.items():
                    if any(kw in desc for kw in keywords):
                        theme_counts[theme]["count"] += 1
                        if not theme_counts[theme]["example"]:
                            theme_counts[theme]["example"] = (
                                item.get("description", str(item))
                                if isinstance(item, dict)
                                else str(item)
                            )
                        break
        except (json.JSONDecodeError, TypeError):
            continue

    misalignment_themes = sorted(
        [
            {
                "theme": theme.replace("_", " "),
                "count": info["count"],
                "example": info["example"],
            }
            for theme, info in theme_counts.items()
            if info["count"] > 0
        ],
        key=lambda x: -x["count"],
    )

    # --- Behavioral correlations ---
    correlations = []

    # Prompt length vs productivity
    prompt_bins = conn.execute("""
        SELECT
            CASE
                WHEN LENGTH(s.first_prompt) < 100 THEN 'short (<100 chars)'
                WHEN LENGTH(s.first_prompt) < 500 THEN 'medium (100-500 chars)'
                ELSE 'long (>500 chars)'
            END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
        ORDER BY avg_prod DESC
    """).fetchall()
    if len(prompt_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in prompt_bins
        ]
        correlations.append(
            {
                "factor": "First prompt length",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Corrections vs completion
    corr_bins = conn.execute("""
        SELECT
            CASE WHEN f.correction_count = 0 THEN 'zero corrections' ELSE 'has corrections' END as bin,
            SUM(CASE WHEN j.outcome = 'completed' THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as completion_pct,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(corr_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0f}% completion rate, {b[2]:.0%} productivity ({b[3]} sessions)"
            for b in corr_bins
        ]
        correlations.append(
            {
                "factor": "Corrections impact",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # Unique tools vs productivity
    tool_bins = conn.execute("""
        SELECT
            CASE WHEN f.unique_tools_used < 5 THEN 'focused (<5 tools)' ELSE 'broad (5+ tools)' END as bin,
            AVG(j.productivity_ratio) as avg_prod,
            COUNT(*) as n
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        JOIN session_judgments j ON s.session_id = j.session_id
        GROUP BY bin
        HAVING n >= 3
    """).fetchall()
    if len(tool_bins) >= 2:
        parts = [
            f"{b[0]}: {b[1]:.0%} productivity ({b[2]} sessions)" for b in tool_bins
        ]
        correlations.append(
            {
                "factor": "Tool breadth",
                "insight": ". ".join(parts) + ".",
                "type": "tip",
            }
        )

    # --- Worst sessions ---
    worst = conn.execute("""
        SELECT j.session_id, j.misalignment_count, j.outcome,
               SUBSTR(s.first_prompt, 1, 120) as prompt_preview
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.misalignment_count > 0
        ORDER BY j.misalignment_count DESC
        LIMIT 5
    """).fetchall()

    worst_sessions = [
        {
            "session_id": w[0],
            "misalignments": w[1],
            "outcome": w[2],
            "prompt_preview": w[3],
        }
        for w in worst
    ]

    return jsonify(
        {
            "prompt_gaps": prompt_gaps,
            "misalignment_themes": misalignment_themes,
            "behavioral_correlations": correlations,
            "worst_sessions": worst_sessions,
        }
    )


@app.route("/api/skills/dimensions")
def api_skill_dimensions():
    from retro.config import SKILL_DIMENSIONS

    dims = []
    for dim_id in sorted(SKILL_DIMENSIONS.keys(), key=lambda x: int(x[1:])):
        d = SKILL_DIMENSIONS[dim_id]
        dims.append(
            {
                "id": dim_id,
                "name": d["name"],
                "short": d["short"],
                "weight": d["weight"],
                "color": d["color"],
            }
        )
    return jsonify({"dimensions": dims})


@app.route("/api/skills/profile")
def api_skill_profile():
    conn = get_conn()

    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return jsonify({"profile": None})

    cols = [d[0] for d in cursor.description]
    p = _row_to_dict(profile, cols)
    return jsonify({"profile": p})


@app.route("/api/skills/session/<session_id>")
def api_skill_session(session_id):
    conn = get_conn()

    cursor = conn.execute(
        "SELECT * FROM session_skills WHERE session_id = ?", [session_id]
    )
    row = cursor.fetchone()
    if not row:
        return jsonify({"skills": None})

    cols = [d[0] for d in cursor.description]
    return jsonify({"skills": _row_to_dict(row, cols)})


@app.route("/api/skills/nudges")
def api_skill_nudges():
    from retro.config import SKILL_DIMENSIONS

    conn = get_conn()
    rows = conn.execute("""
        SELECT id, dimension, current_level, target_level, nudge_text,
               evidence, frequency, dismissed, created_at
        FROM skill_nudges
        WHERE dismissed = FALSE
        ORDER BY created_at DESC
    """).fetchall()

    cols = [
        "id",
        "dimension",
        "current_level",
        "target_level",
        "nudge_text",
        "evidence",
        "frequency",
        "dismissed",
        "created_at",
    ]

    nudges = []
    for r in rows:
        nd = _row_to_dict(r, cols)
        dim_id = nd.get("dimension", "")
        dim_info = SKILL_DIMENSIONS.get(dim_id, {})
        nd["dimension_name"] = dim_info.get("name", dim_id)
        nd["dimension_color"] = dim_info.get("color", "#8b8fa3")
        nudges.append(nd)

    return jsonify({"nudges": nudges})


@app.route("/api/skills/nudges/<int:nid>/dismiss", methods=["POST"])
def api_dismiss_skill_nudge(nid):
    conn = get_conn()
    conn.execute("UPDATE skill_nudges SET dismissed = TRUE WHERE id = ?", [nid])
    return jsonify({"ok": True})


@app.route("/api/skills/dimensions/detail")
def api_skill_dimensions_detail():
    """Return all dimensions with nudge text for next level + example sessions."""
    from retro.config import SKILL_DIMENSIONS, SKILL_NUDGES

    conn = get_conn()

    # Get profile
    cursor = conn.execute("SELECT * FROM skill_profile WHERE id = 1")
    profile = cursor.fetchone()
    if not profile:
        return jsonify({"dimensions": []})
    cols = [d[0] for d in cursor.description]
    p = _row_to_dict(profile, cols)
    gaps = [p.get("gap_1"), p.get("gap_2"), p.get("gap_3")]

    results = []
    for dim_id in sorted(SKILL_DIMENSIONS.keys(), key=lambda x: int(x[1:])):
        d = SKILL_DIMENSIONS[dim_id]
        num = int(dim_id[1:])
        score = p.get(f"d{num}_score", 0) or 0
        level = int(score)
        is_gap = dim_id in gaps
        target = level + 1

        # Nudge text for next level (works for ALL dimensions, not just gaps)
        nudge = SKILL_NUDGES.get((dim_id, target), "")

        # Find example sessions: best demos (high level) and opportunities (high opp)
        level_col = f"d{num}_level"
        opp_col = f"d{num}_opportunity"
        examples = conn.execute(f"""
            SELECT sk.session_id, sk.{level_col}, sk.{opp_col},
                   s.first_prompt, s.started_at, s.duration_seconds,
                   s.project_name, j.outcome, j.productivity_ratio
            FROM session_skills sk
            JOIN sessions s ON sk.session_id = s.session_id
            LEFT JOIN session_judgments j ON sk.session_id = j.session_id
            WHERE (sk.{level_col} >= 2 OR sk.{opp_col} > sk.{level_col})
              AND s.turn_count >= 1
              AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
            ORDER BY sk.{level_col} DESC, s.started_at DESC
            LIMIT 5
        """).fetchall()

        example_sessions = []
        for sid, lv, opp, prompt, started, dur, project, outcome, prod in examples:
            label = f"L{lv}"
            if opp > lv:
                label += f" (could be L{opp})"
            short_project = (project or "").replace("-Users-npow-code-", "").replace("-Users-npow-", "")
            example_sessions.append({
                "session_id": sid,
                "level": lv,
                "opportunity": opp,
                "label": label,
                "first_prompt": (prompt or "")[:80],
                "started_at": _serialize(started),
                "duration": dur,
                "project": short_project,
                "outcome": outcome,
                "productivity": prod,
            })

        results.append({
            "id": dim_id,
            "name": d["name"],
            "short": d["short"],
            "color": d["color"],
            "score": round(score, 1),
            "level": level,
            "is_gap": is_gap,
            "next_level": target,
            "nudge": nudge,
            "examples": example_sessions,
        })

    return jsonify({"dimensions": results})


@app.route("/api/synthesis")
def api_synthesis():
    """Return the cross-session synthesis report."""
    conn = get_conn()
    row = conn.execute("SELECT * FROM synthesis WHERE id = 1").fetchone()
    if not row:
        return jsonify({"synthesis": None})

    cols = [d[0] for d in conn.execute("SELECT * FROM synthesis WHERE id = 1").description]
    result = _row_to_dict(row, cols)

    # Parse JSON fields
    for field in ("at_a_glance", "top_wins", "top_friction", "claude_md_additions"):
        if result.get(field) and isinstance(result[field], str):
            try:
                result[field] = json.loads(result[field])
            except (json.JSONDecodeError, ValueError):
                pass

    return jsonify({"synthesis": result})


@app.route("/api/sessions/<session_id>/narrative")
def api_session_narrative(session_id):
    """Return the rich narrative for a session."""
    conn = get_conn()
    row = conn.execute(
        """SELECT narrative, what_worked, what_failed, user_quote,
                  claude_md_suggestion, claude_md_rationale, prompt_summary
           FROM session_judgments WHERE session_id = ?""",
        [session_id],
    ).fetchone()

    if not row:
        return jsonify({"narrative": None})

    return jsonify({
        "narrative": {
            "narrative": row[0],
            "what_worked": row[1],
            "what_failed": row[2],
            "user_quote": row[3],
            "claude_md_suggestion": row[4],
            "claude_md_rationale": row[5],
            "prompt_summary": row[6],
        }
    })


@app.route("/api/claude-md-suggestions")
def api_claude_md_suggestions():
    """Return all CLAUDE.md suggestions with copy-ready text."""
    conn = get_conn()
    rows = conn.execute("""
        SELECT j.session_id, j.claude_md_suggestion, j.claude_md_rationale,
               j.prompt_summary, s.project_name, s.started_at
        FROM session_judgments j
        JOIN sessions s ON j.session_id = s.session_id
        WHERE j.claude_md_suggestion IS NOT NULL AND j.claude_md_suggestion != ''
        ORDER BY s.started_at DESC
    """).fetchall()

    # Also include synthesis-level suggestions
    synthesis_suggestions = []
    synth = conn.execute(
        "SELECT claude_md_additions FROM synthesis WHERE id = 1"
    ).fetchone()
    if synth and synth[0]:
        try:
            additions = json.loads(synth[0]) if isinstance(synth[0], str) else synth[0]
            for a in additions:
                synthesis_suggestions.append({
                    "rule": a.get("rule", ""),
                    "rationale": a.get("rationale", ""),
                    "evidence": a.get("evidence", ""),
                    "source": "synthesis",
                })
        except (json.JSONDecodeError, ValueError):
            pass

    session_suggestions = []
    for r in rows:
        session_suggestions.append({
            "session_id": r[0],
            "rule": r[1],
            "rationale": r[2],
            "prompt_summary": r[3],
            "project_name": r[4],
            "started_at": _serialize(r[5]),
            "source": "session",
        })

    return jsonify({
        "synthesis_suggestions": synthesis_suggestions,
        "session_suggestions": session_suggestions,
    })


@app.route("/api/session-highlights")
def api_session_highlights():
    """Return top noteworthy sessions with their narratives."""
    conn = get_conn()

    _filter = """
        JOIN sessions s ON j.session_id = s.session_id
        WHERE s.turn_count >= 1
          AND s.first_prompt NOT LIKE 'You are analyzing a Claude Code session%'
    """

    highlights = []

    # Most productive session
    row = conn.execute(f"""
        SELECT j.session_id, j.productivity_ratio, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.outcome = 'completed'
        ORDER BY j.productivity_ratio DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append({
            "type": "most_productive",
            "label": "Most Productive",
            "session_id": row[0], "productivity": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_worked": row[8],
        })

    # Most wasteful session
    row = conn.execute(f"""
        SELECT j.session_id, j.productivity_ratio, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_failed, j.misalignment_count
        FROM session_judgments j {_filter}
          AND j.waste_turns > 0
        ORDER BY j.waste_turns DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append({
            "type": "most_wasteful",
            "label": "Most Wasteful",
            "session_id": row[0], "productivity": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_failed": row[8], "misalignments": row[9],
        })

    # Most misaligned session
    row = conn.execute(f"""
        SELECT j.session_id, j.misalignment_count, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.duration_seconds,
               j.what_failed
        FROM session_judgments j {_filter}
          AND j.misalignment_count > 0
        ORDER BY j.misalignment_count DESC LIMIT 1
    """).fetchone()
    if row and (not highlights or row[0] != highlights[-1].get("session_id")):
        highlights.append({
            "type": "most_misaligned",
            "label": "Most Misaligned",
            "session_id": row[0], "misalignments": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "duration": row[7], "what_failed": row[8],
        })

    # Best prompt quality
    row = conn.execute(f"""
        SELECT j.session_id, j.prompt_clarity, j.prompt_completeness, j.outcome,
               j.narrative, j.prompt_summary, s.project_name, s.started_at,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.prompt_clarity >= 0.8 AND j.prompt_completeness >= 0.8
          AND j.outcome = 'completed'
        ORDER BY (j.prompt_clarity + j.prompt_completeness) DESC LIMIT 1
    """).fetchone()
    if row:
        highlights.append({
            "type": "best_prompt",
            "label": "Best Prompt",
            "session_id": row[0], "clarity": row[1], "completeness": row[2],
            "outcome": row[3], "narrative": row[4], "prompt_summary": row[5],
            "project": row[6], "started_at": _serialize(row[7]),
            "what_worked": row[8],
        })

    # Longest successful session
    row = conn.execute(f"""
        SELECT j.session_id, s.duration_seconds, j.outcome, j.narrative,
               j.prompt_summary, s.project_name, s.started_at, s.turn_count,
               j.what_worked
        FROM session_judgments j {_filter}
          AND j.outcome = 'completed'
        ORDER BY s.duration_seconds DESC LIMIT 1
    """).fetchone()
    if row and (not highlights or row[0] != highlights[0].get("session_id")):
        highlights.append({
            "type": "longest_success",
            "label": "Longest Success",
            "session_id": row[0], "duration": row[1], "outcome": row[2],
            "narrative": row[3], "prompt_summary": row[4],
            "project": row[5], "started_at": _serialize(row[6]),
            "turns": row[7], "what_worked": row[8],
        })

    return jsonify({"highlights": highlights[:5]})


@app.route("/api/heatmap")
def api_heatmap():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            f.day_of_week,
            f.hour_of_day,
            COUNT(*) as count,
            AVG(s.convergence_score) as avg_convergence
        FROM sessions s
        JOIN session_features f ON s.session_id = f.session_id
        GROUP BY f.day_of_week, f.hour_of_day
        ORDER BY f.day_of_week, f.hour_of_day
    """).fetchall()

    return jsonify(
        {
            "heatmap": [
                {
                    "day": r[0],
                    "hour": r[1],
                    "count": r[2],
                    "avg_convergence": round(r[3], 3),
                }
                for r in rows
            ],
        }
    )


@app.route("/api/heatmap/calendar")
def api_heatmap_calendar():
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            DATE(started_at) as day,
            COUNT(*) as count
        FROM sessions
        WHERE turn_count >= 1
          AND first_prompt NOT LIKE 'You are analyzing a Claude Code session%%'
          AND started_at >= DATE('now', '-365 days')
        GROUP BY DATE(started_at)
        ORDER BY day
    """).fetchall()

    return jsonify(
        {
            "calendar": [
                {"date": str(r[0]), "count": r[1]}
                for r in rows
            ],
        }
    )


def _build_monitor_data():
    """Query DB and return list of session dicts for the monitor view."""
    from agenttrace.db import get_writer
    # Ensure schema is initialized (creates tables if missing)
    get_writer()
    conn = get_conn()

    rows = conn.execute("""
        SELECT
            s.session_id,
            s.project_name,
            s.started_at,
            s.turn_count,
            s.tool_use_count,
            s.tool_error_count,
            s.duration_seconds,
            MAX(r.timestamp_utc) AS last_activity,
            COALESCE(sa.subagent_spawn_count, 0) AS subagent_spawn_count,
            COALESCE(bh.bash_heartbeat_count, 0) AS bash_heartbeat_count
        FROM sessions s
        LEFT JOIN raw_entries r ON r.session_id = s.session_id
        LEFT JOIN (
            SELECT session_id,
                   COUNT(DISTINCT parent_tool_id) AS subagent_spawn_count
            FROM progress_entries
            WHERE progress_type = 'agent_progress'
            GROUP BY session_id
        ) sa ON sa.session_id = s.session_id
        LEFT JOIN (
            SELECT session_id,
                   COUNT(*) AS bash_heartbeat_count
            FROM progress_entries
            WHERE progress_type = 'bash_progress'
            GROUP BY session_id
        ) bh ON bh.session_id = s.session_id
        GROUP BY s.session_id
        ORDER BY last_activity DESC NULLS LAST
    """).fetchall()

    sessions = []
    for row in rows:
        (session_id, project_name, started_at, turn_count, tool_use_count,
         tool_error_count, duration_seconds, last_activity,
         subagent_spawn_count, bash_heartbeat_count) = row

        # Determine active status: active if last activity within 5 minutes
        is_active = False
        if last_activity:
            from datetime import datetime, timezone
            try:
                if isinstance(last_activity, str):
                    # Parse ISO timestamp
                    last_dt = datetime.fromisoformat(last_activity.replace("Z", "+00:00"))
                    if last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                else:
                    last_dt = last_activity
                    if hasattr(last_dt, 'tzinfo') and last_dt.tzinfo is None:
                        last_dt = last_dt.replace(tzinfo=timezone.utc)
                now = datetime.now(timezone.utc)
                diff_seconds = (now - last_dt).total_seconds()
                is_active = diff_seconds < 300
            except Exception:
                pass

        # Human-readable duration
        dur = duration_seconds or 0
        if dur >= 3600:
            hours = dur // 3600
            mins = (dur % 3600) // 60
            secs = dur % 60
            duration_str = f"{hours}h {mins}m {secs}s"
        elif dur >= 60:
            mins = dur // 60
            secs = dur % 60
            duration_str = f"{mins}m {secs}s"
        else:
            duration_str = f"{dur}s"

        sessions.append({
            "session_id": session_id,
            "session_id_short": (session_id or "")[:12],
            "project_name": project_name or "",
            "status": "active" if is_active else "idle",
            "turn_count": turn_count or 0,
            "tool_use_count": tool_use_count or 0,
            "tool_error_count": tool_error_count or 0,
            "subagent_spawn_count": subagent_spawn_count or 0,
            "bash_heartbeat_count": bash_heartbeat_count or 0,
            "duration": duration_str,
            "started_at": started_at or "",
            "last_activity": last_activity or "",
            "is_active": is_active,
        })

    return sessions


@app.route("/api/monitor")
def api_monitor():
    """JSON endpoint returning session monitor data."""
    sessions = _build_monitor_data()
    total_sessions = len(sessions)
    active_count = sum(1 for s in sessions if s["is_active"])
    total_tool_calls = sum(s["tool_use_count"] for s in sessions)
    total_subagent_spawns = sum(s["subagent_spawn_count"] for s in sessions)

    return jsonify({
        "sessions": sessions,
        "summary": {
            "total_sessions": total_sessions,
            "active_count": active_count,
            "total_tool_calls": total_tool_calls,
            "total_subagent_spawns": total_subagent_spawns,
        },
    })


@app.route("/monitor")
def monitor():
    """Interactive session monitor SPA."""

    html = """<!DOCTYPE html>
<html lang="en" data-theme="light">
<head>
<meta charset="UTF-8">
<meta name="viewport" content="width=device-width, initial-scale=1.0">
<title>AgentTrace Monitor</title>
<style>
/* ── theme variables ── */
:root[data-theme="light"] {
  --bg:        #ffffff;
  --bg-alt:    #f6f8fa;
  --bg-row:    #ffffff;
  --bg-hover:  #f0f4f8;
  --bg-sel:    #ddf4e8;
  --bg-active: #f0fff4;
  --bg-error:  #fff0f0;
  --bg-detail: #fafbfc;
  --bg-code:   #f6f8fa;
  --bg-input:  #ffffff;
  --border:    #d0d7de;
  --border-sub:#e1e4e8;
  --text:      #24292f;
  --text-dim:  #57606a;
  --text-muted:#8c959f;
  --text-code: #0550ae;
  --accent:    #1a7f37;
  --accent-bg: #d1f0da;
  --warn:      #9a6700;
  --warn-bg:   #fff8c5;
  --err:       #cf222e;
  --err-bg:    #ffebe9;
  --sel-border:#1a7f37;
  --shadow:    0 1px 3px rgba(0,0,0,.12);
  --chip-bg:   #f0f4f8;
  --chip-border:#c8d0d8;
  --tl-user:   #0969da;
  --tl-asst:   #8250df;
  --tl-sys:    #8c959f;
}
:root[data-theme="dark"] {
  --bg:        #0d1117;
  --bg-alt:    #161b22;
  --bg-row:    #0d1117;
  --bg-hover:  #1c2128;
  --bg-sel:    #0f2d1f;
  --bg-active: #0d1f14;
  --bg-error:  #1f0d0d;
  --bg-detail: #0d1117;
  --bg-code:   #161b22;
  --bg-input:  #161b22;
  --border:    #30363d;
  --border-sub:#21262d;
  --text:      #e6edf3;
  --text-dim:  #8b949e;
  --text-muted:#484f58;
  --text-code: #79c0ff;
  --accent:    #3fb950;
  --accent-bg: #0f2d1f;
  --warn:      #d29922;
  --warn-bg:   #271f00;
  --err:       #f85149;
  --err-bg:    #2d0d0d;
  --sel-border:#3fb950;
  --shadow:    0 1px 4px rgba(0,0,0,.4);
  --chip-bg:   #21262d;
  --chip-border:#30363d;
  --tl-user:   #58a6ff;
  --tl-asst:   #bc8cff;
  --tl-sys:    #484f58;
}

/* ── reset & base ── */
* { box-sizing: border-box; margin: 0; padding: 0; }
body {
  background: var(--bg); color: var(--text);
  font-family: -apple-system, BlinkMacSystemFont, 'Segoe UI', Helvetica, Arial, sans-serif;
  font-size: 13px; height: 100vh; display: flex; flex-direction: column; overflow: hidden;
}
code, .mono { font-family: ui-monospace, 'SFMono-Regular', Consolas, monospace; }

/* ── topbar ── */
#topbar {
  background: var(--bg-alt); border-bottom: 1px solid var(--border);
  padding: 8px 16px; display: flex; gap: 20px; align-items: center;
  flex-shrink: 0; box-shadow: var(--shadow);
}
.brand {
  font-weight: 700; font-size: 13px; letter-spacing: .5px;
  color: var(--accent); white-space: nowrap;
}
.stat { display: flex; flex-direction: column; gap: 1px; white-space: nowrap; }
.stat-label { color: var(--text-muted); font-size: 10px; text-transform: uppercase; letter-spacing: .8px; }
.stat-value { font-weight: 600; font-size: 13px; color: var(--text); }
.stat-value.green { color: var(--accent); }
.stat-value.amber { color: var(--warn); }
.stat-value.red   { color: var(--err); }
#controls { margin-left: auto; display: flex; gap: 8px; align-items: center; }
#filter-input {
  background: var(--bg-input); border: 1px solid var(--border);
  color: var(--text); padding: 5px 10px; font-size: 12px; border-radius: 6px;
  width: 200px; font-family: inherit;
}
#filter-input:focus { outline: none; border-color: var(--accent); }
#filter-input::placeholder { color: var(--text-muted); }
.ctrl-label {
  display: flex; align-items: center; gap: 5px; cursor: pointer;
  font-size: 12px; color: var(--text-dim); white-space: nowrap;
}
.ctrl-label input { cursor: pointer; accent-color: var(--accent); }
#theme-btn {
  background: var(--bg-input); border: 1px solid var(--border);
  color: var(--text-dim); cursor: pointer; padding: 5px 10px;
  font-size: 12px; border-radius: 6px; font-family: inherit;
}
#theme-btn:hover { border-color: var(--accent); color: var(--text); }
#refresh-status { font-size: 11px; color: var(--text-muted); min-width: 110px; text-align: right; }

/* ── main split ── */
#main { display: flex; flex: 1; overflow: hidden; }

/* ── session list ── */
#list-panel {
  width: 480px; flex-shrink: 0; border-right: 1px solid var(--border);
  display: flex; flex-direction: column; overflow: hidden; background: var(--bg-alt);
}
#list-table-wrap { flex: 1; overflow-y: auto; }
table { width: 100%; border-collapse: collapse; }
thead { position: sticky; top: 0; z-index: 1; background: var(--bg-alt); }
thead tr { border-bottom: 2px solid var(--border); }
th {
  color: var(--text-muted); font-size: 11px; font-weight: 600;
  text-transform: uppercase; letter-spacing: .6px;
  padding: 6px 10px; text-align: left; white-space: nowrap;
  cursor: pointer; user-select: none;
}
th:hover { color: var(--text-dim); }
th.sort-asc::after  { content: ' ↑'; color: var(--accent); }
th.sort-desc::after { content: ' ↓'; color: var(--accent); }
th.num { text-align: right; }
tbody tr {
  cursor: pointer; border-bottom: 1px solid var(--border-sub);
  background: var(--bg-row); transition: background .08s;
}
tbody tr:hover   { background: var(--bg-hover); }
tbody tr.active-row { background: var(--bg-active); }
tbody tr.error-row  { background: var(--bg-error); }
tbody tr.selected   {
  background: var(--bg-sel) !important;
  border-left: 3px solid var(--sel-border);
}
td { padding: 5px 10px; vertical-align: middle; }
td.num  { text-align: right; color: var(--text-dim); font-variant-numeric: tabular-nums; }
td.err  { color: var(--err); font-weight: 600; }
td.mono { color: var(--text-muted); font-size: 11px; }
.status-pill {
  display: inline-block; padding: 1px 7px; border-radius: 12px;
  font-size: 10px; font-weight: 700; letter-spacing: .5px; text-transform: uppercase;
}
.pill-active { background: var(--accent-bg); color: var(--accent); }
.pill-idle   { background: var(--bg-code);   color: var(--text-muted); }
.pill-error  { background: var(--err-bg);    color: var(--err); }
.proj-name {
  font-weight: 500; max-width: 220px;
  overflow: hidden; text-overflow: ellipsis; white-space: nowrap;
}

/* ── detail panel ── */
#detail-panel {
  flex: 1; overflow-y: auto; padding: 20px 24px; background: var(--bg-detail);
}
#detail-panel .placeholder {
  display: flex; align-items: center; justify-content: center;
  height: 100%; color: var(--text-muted); font-size: 14px;
}
.dsec { margin-bottom: 24px; }
.dsec-title {
  font-size: 11px; font-weight: 600; text-transform: uppercase;
  letter-spacing: .8px; color: var(--text-muted);
  margin-bottom: 10px; padding-bottom: 6px; border-bottom: 1px solid var(--border);
}
.dheader { display: flex; gap: 12px; align-items: center; flex-wrap: wrap; margin-bottom: 16px; }
.dproj { font-size: 18px; font-weight: 700; color: var(--text); }
.did   { font-size: 11px; color: var(--text-muted); font-family: ui-monospace, monospace; }
.badge {
  display: inline-block; padding: 2px 8px; border-radius: 12px;
  font-size: 11px; font-weight: 600; border: 1px solid;
}
.badge-ok   { color: var(--accent); border-color: var(--accent); background: var(--accent-bg); }
.badge-warn { color: var(--warn);   border-color: var(--warn);   background: var(--warn-bg);   }
.badge-err  { color: var(--err);    border-color: var(--err);    background: var(--err-bg);    }
.badge-neu  { color: var(--text-dim); border-color: var(--border); background: var(--bg-code); }

/* metrics row */
.metrics { display: flex; gap: 0; margin-bottom: 20px; border: 1px solid var(--border); border-radius: 8px; overflow: hidden; }
.metric {
  flex: 1; padding: 10px 14px; display: flex; flex-direction: column; gap: 3px;
  border-right: 1px solid var(--border); background: var(--bg-alt);
}
.metric:last-child { border-right: none; }
.metric-label { font-size: 10px; text-transform: uppercase; letter-spacing: .6px; color: var(--text-muted); font-weight: 600; }
.metric-value { font-size: 18px; font-weight: 700; color: var(--text); }
.metric-value.hi  { color: var(--warn); }
.metric-value.err { color: var(--err); }
.metric-value.ok  { color: var(--accent); }
.metric-sub { font-size: 11px; color: var(--text-muted); }

/* first prompt */
.first-prompt {
  background: var(--bg-code); border: 1px solid var(--border); border-radius: 6px;
  padding: 12px 14px; color: var(--text); font-size: 13px; line-height: 1.6;
  white-space: pre-wrap; word-break: break-word; max-height: 140px; overflow-y: auto;
}

/* tool chips */
.tool-grid { display: flex; flex-wrap: wrap; gap: 6px; }
.tool-chip {
  display: inline-flex; align-items: center; gap: 6px;
  background: var(--chip-bg); border: 1px solid var(--chip-border);
  padding: 4px 10px; border-radius: 16px; font-size: 12px;
}
.tc-name  { color: var(--text); font-weight: 500; }
.tc-count { color: var(--accent); font-weight: 700; font-size: 11px; }
.tc-err   { color: var(--err); font-size: 11px; font-weight: 600; }

/* timeline */
.tl-entry {
  display: flex; gap: 12px; padding: 7px 0; border-bottom: 1px solid var(--border-sub);
  align-items: flex-start;
}
.tl-type {
  flex-shrink: 0; width: 64px; font-size: 10px; font-weight: 700;
  text-transform: uppercase; letter-spacing: .6px; padding-top: 1px;
}
.tl-type.user      { color: var(--tl-user); }
.tl-type.assistant { color: var(--tl-asst); }
.tl-type.system    { color: var(--tl-sys); }
.tl-body  { flex: 1; min-width: 0; }
.tl-preview { color: var(--text-dim); font-size: 12px; line-height: 1.5; }
.tl-tools   { color: var(--warn); font-size: 11px; margin-top: 3px; font-weight: 500; }
.tl-time { flex-shrink: 0; color: var(--text-muted); font-size: 11px; padding-top: 1px; font-family: ui-monospace,monospace; }

/* judgment */
.judgment-body { font-size: 13px; line-height: 1.7; color: var(--text-dim); }
.judgment-kv { margin-top: 10px; }
.kv-label { font-size: 11px; font-weight: 600; text-transform: uppercase; letter-spacing: .6px; color: var(--text-muted); margin-bottom: 3px; }
.kv-val { font-size: 13px; color: var(--text); line-height: 1.5; }
.judgment-quote {
  color: var(--text-dim); font-style: italic; border-left: 3px solid var(--border);
  padding-left: 12px; margin-top: 12px; font-size: 13px; line-height: 1.6;
}
</style>
</head>
<body>
<div id="topbar">
  <span class="brand">⬡ AgentTrace Monitor</span>
  <div class="stat"><span class="stat-label">Sessions</span><span class="stat-value" id="s-total">–</span></div>
  <div class="stat"><span class="stat-label">Active</span><span class="stat-value green" id="s-active">–</span></div>
  <div class="stat"><span class="stat-label">Tools</span><span class="stat-value amber" id="s-tools">–</span></div>
  <div class="stat"><span class="stat-label">Subagents</span><span class="stat-value" id="s-subagents">–</span></div>
  <div class="stat"><span class="stat-label">Errors</span><span class="stat-value red" id="s-errors">–</span></div>
  <div id="controls">
    <input id="filter-input" type="text" placeholder="Filter by project…" />
    <label class="ctrl-label"><input type="checkbox" id="active-cb" /> Active only</label>
    <button id="theme-btn" onclick="toggleTheme()">☀ Light</button>
    <span id="refresh-status">connecting…</span>
  </div>
</div>
<div id="main">
  <div id="list-panel">
    <div id="list-table-wrap">
      <table id="session-table">
        <thead>
          <tr>
            <th data-col="status" style="width:70px">Status</th>
            <th data-col="project_name">Project</th>
            <th data-col="turn_count"  class="num">Turns</th>
            <th data-col="tool_use_count" class="num">Tools</th>
            <th data-col="tool_error_count" class="num">Err</th>
            <th data-col="subagent_spawn_count" class="num">Sub</th>
            <th data-col="duration_seconds" class="num">Age</th>
          </tr>
        </thead>
        <tbody id="session-tbody"></tbody>
      </table>
    </div>
  </div>
  <div id="detail-panel">
    <div class="placeholder">Select a session to see details →</div>
  </div>
</div>

<script>
const state = {
  sessions: [], selected: null,
  sortCol: 'last_activity', sortDir: -1,
  filter: '', activeOnly: false,
};

// ── theme ─────────────────────────────────────────────────────────────────────
let theme = localStorage.getItem('at-theme') || 'light';
function applyTheme(t) {
  theme = t;
  document.documentElement.setAttribute('data-theme', t);
  document.getElementById('theme-btn').textContent = t === 'dark' ? '☀ Light' : '☾ Dark';
  localStorage.setItem('at-theme', t);
}
function toggleTheme() { applyTheme(theme === 'dark' ? 'light' : 'dark'); }
applyTheme(theme);

// ── data ──────────────────────────────────────────────────────────────────────

async function fetchSessions() {
  try {
    const r = await fetch('/api/monitor');
    const d = await r.json();
    state.sessions = d.sessions;
    renderList();
    const sum = d.summary;
    document.getElementById('s-total').textContent = sum.total_sessions;
    document.getElementById('s-active').textContent = sum.active_count;
    document.getElementById('s-tools').textContent = sum.total_tool_calls.toLocaleString();
    document.getElementById('s-subagents').textContent = sum.total_subagent_spawns.toLocaleString();
    const totalErr = state.sessions.reduce((a, s) => a + s.tool_error_count, 0);
    document.getElementById('s-errors').textContent = totalErr;
    document.getElementById('refresh-status').textContent = '↻ ' + new Date().toLocaleTimeString();
  } catch(e) {
    document.getElementById('refresh-status').textContent = '✗ ' + e.message;
  }
}

async function loadDetail(sessionId) {
  const dp = document.getElementById('detail-panel');
  dp.innerHTML = '<div class="placeholder">Loading…</div>';
  try {
    const [dr, tr] = await Promise.all([
      fetch('/api/sessions/' + sessionId),
      fetch('/api/sessions/' + sessionId + '/timeline'),
    ]);
    const detail = await dr.json();
    const {timeline} = await tr.json();
    renderDetail(detail, timeline);
  } catch(e) {
    dp.innerHTML = '<div class="placeholder">Error: ' + esc(e.message) + '</div>';
  }
}

// ── list render ───────────────────────────────────────────────────────────────

function filteredSorted() {
  let list = state.sessions;
  if (state.activeOnly) list = list.filter(s => s.is_active);
  if (state.filter) {
    const q = state.filter.toLowerCase();
    list = list.filter(s => s.project_name.toLowerCase().includes(q) || s.session_id.includes(q));
  }
  const col = state.sortCol;
  return [...list].sort((a, b) => {
    let av = a[col] ?? '', bv = b[col] ?? '';
    if (typeof av === 'string') { av = av.toLowerCase(); bv = bv.toLowerCase(); }
    return av < bv ? state.sortDir : av > bv ? -state.sortDir : 0;
  });
}

function projLabel(raw) {
  // Convert -Users-npow-code-foo-bar  →  foo/bar
  return raw.replace(/^-Users-[^-]+-code-/, '').replace(/-/g, '/') || raw;
}

function renderList() {
  const tbody = document.getElementById('session-tbody');
  const list  = filteredSorted();
  tbody.innerHTML = '';
  for (const s of list) {
    const tr = document.createElement('tr');
    if (s.is_active) tr.className = 'active-row';
    else if (s.tool_error_count > 0) tr.className = 'error-row';
    if (s.session_id === state.selected) tr.classList.add('selected');

    const pillCls = s.is_active ? 'pill-active' : (s.tool_error_count > 0 ? 'pill-error' : 'pill-idle');
    const pillTxt = s.is_active ? 'active' : (s.tool_error_count > 0 ? 'error' : 'idle');

    const errCell = s.tool_error_count > 0
      ? `<td class="num err">${s.tool_error_count}</td>`
      : `<td class="num" style="color:var(--text-muted);opacity:.4">–</td>`;
    const subCell = s.subagent_spawn_count > 0
      ? `<td class="num" style="color:var(--accent);font-weight:600">${s.subagent_spawn_count}</td>`
      : `<td class="num" style="color:var(--text-muted);opacity:.4">–</td>`;

    tr.innerHTML = `
      <td><span class="status-pill ${pillCls}">${pillTxt}</span></td>
      <td class="proj-name" title="${esc(s.project_name)}">${esc(projLabel(s.project_name))}</td>
      <td class="num">${s.turn_count}</td>
      <td class="num">${s.tool_use_count.toLocaleString()}</td>
      ${errCell}
      ${subCell}
      <td class="num mono">${s.duration}</td>`;

    tr.addEventListener('click', () => { state.selected = s.session_id; renderList(); loadDetail(s.session_id); });
    tbody.appendChild(tr);
  }
  if (!list.length) {
    tbody.innerHTML = '<tr><td colspan="7" style="padding:32px;text-align:center;color:var(--text-muted)">No sessions match</td></tr>';
  }
}

// ── detail render ─────────────────────────────────────────────────────────────

function renderDetail(detail, timeline) {
  const s      = detail.session || {};
  const tools  = detail.tools   || [];
  const j      = detail.judgment;
  const dp     = document.getElementById('detail-panel');

  const proj  = projLabel(s.project_name || '');
  const errRate = s.tool_use_count > 0
    ? ((s.tool_error_count / s.tool_use_count) * 100).toFixed(1) + '%' : '0%';

  // Header badge
  let hBadge = '';
  if (j) {
    const [cls, lbl] = outcomeBadge(j.outcome);
    hBadge = `<span class="badge ${cls}">${lbl}</span>`;
  }

  // Metrics
  const metrics = `<div class="metrics">
    <div class="metric">
      <div class="metric-label">Turns</div>
      <div class="metric-value">${s.turn_count||0}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Tool calls</div>
      <div class="metric-value hi">${(s.tool_use_count||0).toLocaleString()}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Errors</div>
      <div class="metric-value ${(s.tool_error_count||0)>0?'err':''}">${s.tool_error_count||0}</div>
      <div class="metric-sub">${errRate} error rate</div>
    </div>
    <div class="metric">
      <div class="metric-label">Duration</div>
      <div class="metric-value">${fmtDur(s.duration_seconds||0)}</div>
    </div>
    <div class="metric">
      <div class="metric-label">Convergence</div>
      <div class="metric-value ok">${(s.convergence_score||0).toFixed(2)}</div>
    </div>
  </div>`;

  // First prompt
  const firstPrompt = s.first_prompt
    ? `<div class="dsec"><div class="dsec-title">First Prompt</div>
       <div class="first-prompt">${esc(s.first_prompt)}</div></div>` : '';

  // Tool breakdown
  let toolHtml = '';
  if (tools.length) {
    const chips = tools.slice(0, 24).map(t =>
      `<div class="tool-chip">
        <span class="tc-name">${esc(t.tool_name||'?')}</span>
        <span class="tc-count">${t.use_count}</span>
        ${t.error_count>0?`<span class="tc-err">×${t.error_count}</span>`:''}
      </div>`).join('');
    toolHtml = `<div class="dsec"><div class="dsec-title">Tool Usage</div><div class="tool-grid">${chips}</div></div>`;
  }

  // Judgment
  let judgmentHtml = '';
  if (j) {
    const [cls, lbl] = outcomeBadge(j.outcome);
    const narrative = j.narrative || j.outcome_reasoning || '';
    judgmentHtml = `<div class="dsec">
      <div class="dsec-title">Analysis</div>
      <span class="badge ${cls}" style="margin-bottom:10px;display:inline-block">${lbl}</span>
      ${narrative ? `<div class="judgment-body">${esc(narrative.slice(0,700))}</div>` : ''}
      ${j.what_worked ? `<div class="judgment-kv"><div class="kv-label">What worked</div><div class="kv-val">${esc(j.what_worked.slice(0,300))}</div></div>` : ''}
      ${j.what_failed ? `<div class="judgment-kv"><div class="kv-label">What failed</div><div class="kv-val">${esc(j.what_failed.slice(0,300))}</div></div>` : ''}
      ${j.user_quote  ? `<div class="judgment-quote">"${esc(j.user_quote.slice(0,240))}"</div>` : ''}
    </div>`;
  }

  // Timeline
  const tl = timeline.slice(-30);
  let tlHtml = '';
  if (tl.length) {
    const rows = tl.map(e => {
      const preview = (e.preview || '').trim().slice(0, 150);
      let toolLine = '';
      if (e.tool_names) {
        try {
          const names = (typeof e.tool_names === 'string' ? JSON.parse(e.tool_names) : e.tool_names) || [];
          if (names.length) toolLine = `<div class="tl-tools">⚙ ${names.join(', ')}</div>`;
        } catch(_) {}
      }
      const ts = e.timestamp_utc ? new Date(e.timestamp_utc).toLocaleTimeString() : '';
      return `<div class="tl-entry">
        <div class="tl-type ${e.entry_type||''}">${e.entry_type||'?'}</div>
        <div class="tl-body">
          <div class="tl-preview">${preview ? esc(preview) : '<span style="opacity:.4">—</span>'}${toolLine}</div>
        </div>
        <div class="tl-time">${ts}</div>
      </div>`;
    }).join('');
    tlHtml = `<div class="dsec"><div class="dsec-title">Recent Activity — last ${tl.length} entries</div>${rows}</div>`;
  }

  dp.innerHTML = `
    <div class="dheader">
      <span class="dproj">${esc(proj)}</span>
      <code class="did">${esc(s.session_id||'')}</code>
      ${hBadge}
    </div>
    ${metrics}
    ${firstPrompt}
    ${judgmentHtml}
    ${toolHtml}
    ${tlHtml}`;
}

// ── helpers ───────────────────────────────────────────────────────────────────

function esc(s) {
  return String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');
}
function fmtDur(s) {
  if (s >= 3600) return Math.floor(s/3600) + 'h ' + Math.floor((s%3600)/60) + 'm';
  if (s >= 60)   return Math.floor(s/60) + 'm ' + (s%60) + 's';
  return s + 's';
}
function outcomeBadge(o) {
  if (!o) return ['badge-neu', 'Unknown'];
  const l = o.toLowerCase();
  if (l.includes('success') || l === 'completed') return ['badge-ok',   o.charAt(0).toUpperCase() + o.slice(1)];
  if (l.includes('fail')    || l === 'abandoned') return ['badge-err',  o.charAt(0).toUpperCase() + o.slice(1)];
  return ['badge-warn', o.charAt(0).toUpperCase() + o.slice(1)];
}

// ── controls ──────────────────────────────────────────────────────────────────

document.getElementById('filter-input').addEventListener('input', e => {
  state.filter = e.target.value; renderList();
});
document.getElementById('active-cb').addEventListener('change', e => {
  state.activeOnly = e.target.checked; renderList();
});
document.querySelectorAll('th[data-col]').forEach(th => {
  th.addEventListener('click', () => {
    const col = th.dataset.col;
    state.sortDir = (state.sortCol === col) ? state.sortDir * -1 : -1;
    state.sortCol = col;
    document.querySelectorAll('th').forEach(h => h.classList.remove('sort-asc','sort-desc'));
    th.classList.add(state.sortDir === -1 ? 'sort-desc' : 'sort-asc');
    renderList();
  });
});

// ── init ──────────────────────────────────────────────────────────────────────

fetchSessions();
setInterval(fetchSessions, 5000);
</script>
</body>
</html>"""

    return Response(html, mimetype="text/html")


def create_app():
    """Return the Flask application instance (for testing and WSGI)."""
    return app
