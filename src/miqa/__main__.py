"""
CLI entry point.

Usage:
    uv run -m miqa geo              # crawl GEO
    uv run -m miqa arrayexpress     # crawl ArrayExpress
    uv run -m miqa all              # crawl both

Options:
    --limit N      Process at most N samples per platform (useful for testing)
    --dry-run      Print what would be done without writing to DB or S3
"""

from __future__ import annotations

from typing import Optional

import typer

import miqa.geo as geo
import miqa.arrayexpress as ae
from miqa.utils import setup_logging
from miqa import config


app = typer.Typer(help="DNA methylation sample database crawler")


def _setup():
    setup_logging()
    import logging
    logging.getLogger().setLevel(config.LOG_LEVEL)


@app.command(name="geo")
def geo_cmd(
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions without writing"),
):
    """Crawl GEO for methylation IDAT files."""
    _setup()
    geo.crawl_and_process(conn=None, dry_run=dry_run)


@app.command()
def arrayexpress(
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Max samples to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions without writing"),
):
    """Crawl ArrayExpress for methylation IDAT files."""
    _setup()
    ae.collect_idats(limit=limit, dry_run=dry_run)


@app.command()
def all(
    limit: Optional[int] = typer.Option(None, "--limit", "-n", help="Max samples to process"),
    dry_run: bool = typer.Option(False, "--dry-run", help="Print actions without writing"),
):
    """Crawl both GEO and ArrayExpress."""
    _setup()
    geo.crawl_and_process(conn=None, dry_run=dry_run)
    ae.collect_idats(limit=limit, dry_run=dry_run)


if __name__ == "__main__":
    app()
