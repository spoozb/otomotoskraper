import csv
from pathlib import Path

import click
import structlog

from otomotoskrap.client import OtomotoClient
from otomotoskrap.config import load_config
from otomotoskrap.scraper import scrape_query
from otomotoskrap.storage import append_csv, write_raw_json

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.make_filtering_bound_logger(0),
)

log = structlog.get_logger()


@click.group()
def cli():
    """Otomoto.pl car listing scraper."""
    pass


@cli.command()
@click.option("--config", default="config.yaml", help="Path to config file.")
@click.option("--dry-run", is_flag=True, help="Validate config and show what would be scraped.")
@click.option("--query", multiple=True, help="Run only these queries (by name).")
def run(config: str, dry_run: bool, query: tuple[str, ...]):
    """Run scraper for configured queries."""
    app_config = load_config(config)
    queries = app_config.queries

    if query:
        query_set = set(query)
        queries = [q for q in queries if q.name in query_set]
        missing = query_set - {q.name for q in queries}
        if missing:
            click.echo(f"Warning: unknown queries: {', '.join(missing)}")

    if dry_run:
        click.echo("Dry run - would scrape the following queries:")
        for q in queries:
            click.echo(f"  {q.name}: {q.url} (max {q.max_pages} pages)")
        click.echo(f"\nSettings: delay={app_config.settings.delay_range}s, "
                    f"retries={app_config.settings.max_retries}, "
                    f"output={app_config.settings.output_dir}")
        return

    client = OtomotoClient(
        delay_range=tuple(app_config.settings.delay_range),
        proxy=app_config.settings.proxy,
        max_retries=app_config.settings.max_retries,
    )
    client.start_session()

    total_listings = 0
    total_pages = 0
    total_failed = 0

    try:
        for q in queries:
            click.echo(f"\nScraping: {q.name}")
            result = scrape_query(q, app_config.settings, client=client)

            if result.listings:
                write_raw_json(result.listings, q.name, app_config.settings.output_dir)
                append_csv(result.listings, app_config.settings.output_dir)

            total_listings += len(result.listings)
            total_pages += result.pages_scraped
            total_failed += result.pages_failed

            click.echo(
                f"  {q.name}: {len(result.listings)} listings, "
                f"{result.pages_scraped} pages OK, {result.pages_failed} failed"
            )
    finally:
        client.close()

    click.echo(f"\nDone: {total_listings} listings from {total_pages} pages "
               f"({total_failed} pages failed)")


@cli.command()
@click.option("--config", default="config.yaml", help="Path to config file.")
def stats(config: str):
    """Show dataset statistics."""
    app_config = load_config(config)
    csv_path = Path(app_config.settings.output_dir) / "consolidated" / "all_listings.csv"

    if not csv_path.exists():
        click.echo("No data found. Run the scraper first.")
        return

    with open(csv_path) as f:
        reader = csv.DictReader(f)
        rows = list(reader)

    if not rows:
        click.echo("No data found. CSV is empty.")
        return

    dates = sorted({r["scraped_at"][:10] for r in rows if r.get("scraped_at")})
    queries = sorted({r["query_name"] for r in rows if r.get("query_name")})
    unique_ids = len({r["listing_id"] for r in rows})

    click.echo(f"Total rows: {len(rows)}")
    click.echo(f"Unique listings: {unique_ids}")
    click.echo(f"Date range: {dates[0]} to {dates[-1]}" if dates else "No dates")
    click.echo(f"Queries: {', '.join(queries)}" if queries else "No queries")
    click.echo(f"Scrape days: {len(dates)}")
