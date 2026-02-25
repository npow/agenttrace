"""CLI entry point: app, serve, ingest, digest, reset."""

import sys


def main():
    args = sys.argv[1:]
    command = args[0] if args else "serve"

    if command == "ingest":
        from agenttrace.ingest import run_ingest
        from retro.sessions import build_sessions, build_tool_usage
        from retro.features import extract_features
        from retro.skills import assess_skills
        from retro.scoring import compute_scores
        from retro.intents import classify_all_intents
        from retro.baselines import compute_baselines
        from retro.prescriptions import generate_prescriptions
        from retro.llm_judge import judge_sessions

        print("Ingesting JSONL files...")
        stats = run_ingest()
        print(
            f"  Files: {stats['total_files']} total, {stats['ingested_files']} ingested, {stats['skipped_files']} skipped"
        )
        print(
            f"  Entries: {stats['total_entries']} new, {stats['total_entries_in_db']} total in DB"
        )
        print(f"  Sessions found: {stats['total_sessions_found']}")
        print(f"  Projects: {stats['total_projects']}")

        print("Building sessions...")
        n = build_sessions()
        print(f"  {n} sessions built")

        print("Building tool usage...")
        n = build_tool_usage()
        print(f"  {n} tool usage records")

        print("Extracting features...")
        n = extract_features()
        print(f"  {n} sessions processed")

        print("Assessing skills...")
        n = assess_skills()
        print(f"  {n} sessions assessed")

        print("Computing scores...")
        n = compute_scores()
        print(f"  {n} sessions scored")

        print("Classifying intents...")
        n = classify_all_intents()
        print(f"  {n} sessions classified")

        print("Judging sessions (LLM analysis)...")
        n = judge_sessions()
        print(f"  {n} sessions judged")

        print("Computing baselines...")
        compute_baselines()
        print("  Done")

        print("Generating prescriptions...")
        n = generate_prescriptions()
        print(f"  {n} prescriptions generated")

        print("\nIngestion complete!")

    elif command == "app":
        from retro.app import launch

        launch()

    elif command == "serve":
        import webbrowser
        from retro.config import SERVER_PORT
        from retro.server import app, set_worker
        from agenttrace.watcher import IngestionWorker

        # Check if DB is empty — worker will run pipeline immediately
        from agenttrace.db import get_conn, get_writer

        # Ensure schema exists by calling get_writer() first
        get_writer()

        conn = get_conn()
        try:
            count = conn.execute("SELECT COUNT(*) FROM raw_entries").fetchone()[0]
            needs_ingest = count == 0
        except:
            needs_ingest = True
        if needs_ingest:
            print("No data found. Ingesting in background...")

        # Start background worker
        worker = IngestionWorker(run_immediately=needs_ingest)
        set_worker(worker)
        worker.start()

        url = f"http://localhost:{SERVER_PORT}"
        print(f"Starting server on {url}")
        webbrowser.open(url)
        app.run(host="127.0.0.1", port=SERVER_PORT, debug=False, threaded=False)

    elif command == "digest":
        from retro.digest import weekly_digest

        print(weekly_digest())

    elif command == "reset":
        from agenttrace.config import DB_PATH

        if DB_PATH.exists():
            DB_PATH.unlink()
            print(f"Deleted {DB_PATH}")
        else:
            print("No database to reset.")

    else:
        print("Usage: python -m retro [app|ingest|serve|digest|reset]")
        sys.exit(1)


if __name__ == "__main__":
    main()
