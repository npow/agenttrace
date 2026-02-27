"""CLI entry point: sessionlog start / status / stop."""

import click


@click.group()
def cli():
    """sessionlog — real-time ingestion for AI coding agent sessions."""


@cli.command()
@click.option("--db", default="~/.sessionlog/data.sqlite", show_default=True, help="SQLite database path")
@click.option("--sources-dir", default="~/.claude/projects", show_default=True, help="Directory to watch for session files")
def start(db: str, sources_dir: str):
    """Start the ingestion daemon."""
    from sessionlog.watcher import IngestionWorker

    click.echo(f"Watching {sources_dir} → {db}")
    worker = IngestionWorker(run_immediately=True)
    worker.start()
    try:
        worker.join()
    except KeyboardInterrupt:
        worker.stop()


@cli.command()
def status():
    """Show ingestion daemon status."""
    click.echo("Not implemented yet")


@cli.command()
@click.option("--force", is_flag=True, default=False, help="Re-ingest all files, ignoring the ingestion log.")
def ingest(force: bool):
    """Run a one-shot incremental ingestion of all JSONL files."""
    from sessionlog.db import get_writer
    from sessionlog.ingest import run_ingest

    if force:
        conn = get_writer()
        conn.execute("DELETE FROM ingestion_log")
        conn.commit()
        click.echo("Cleared ingestion log — will re-ingest all files.")

    stats = run_ingest()
    click.echo(
        f"Done. "
        f"{stats['ingested_files']}/{stats['total_files']} files ingested, "
        f"{stats['total_entries']} raw entries, "
        f"{stats['total_progress_entries']} progress entries "
        f"({stats['skipped_files']} skipped, {stats['failed_files']} failed). "
        f"DB totals: {stats['total_entries_in_db']} entries, "
        f"{stats['total_sessions_found']} sessions, "
        f"{stats['total_projects']} projects."
    )


if __name__ == "__main__":
    cli()
