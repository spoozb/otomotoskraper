import json
from pathlib import Path

import yaml
from click.testing import CliRunner

from otomotoskrap.cli import cli


def _write_config(tmp_path, queries=None):
    """Write a test config file and return its path."""
    config_path = tmp_path / "config.yaml"
    config_data = {
        "queries": queries
        or [
            {
                "name": "test_bmw",
                "url": "https://www.otomoto.pl/osobowe/bmw",
                "max_pages": 1,
            }
        ],
        "settings": {
            "delay_range": [0, 0],
            "output_dir": str(tmp_path / "data"),
        },
    }
    config_path.write_text(yaml.dump(config_data))
    return str(config_path)


class TestDryRun:
    def test_dry_run_shows_queries(self, tmp_path):
        config = _write_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", config, "--dry-run"])
        assert result.exit_code == 0
        assert "test_bmw" in result.output
        assert "Dry run" in result.output or "dry run" in result.output.lower()

    def test_dry_run_with_query_filter(self, tmp_path):
        config = _write_config(
            tmp_path,
            queries=[
                {"name": "bmw", "url": "https://www.otomoto.pl/osobowe/bmw", "max_pages": 1},
                {"name": "audi", "url": "https://www.otomoto.pl/osobowe/audi", "max_pages": 1},
            ],
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["run", "--config", config, "--dry-run", "--query", "bmw"])
        assert result.exit_code == 0
        assert "bmw" in result.output


class TestStats:
    def test_stats_no_data(self, tmp_path):
        config = _write_config(tmp_path)
        runner = CliRunner()
        result = runner.invoke(cli, ["stats", "--config", config])
        assert result.exit_code == 0
        assert "No data" in result.output or "0" in result.output

    def test_stats_with_data(self, tmp_path):
        config = _write_config(tmp_path)
        # Create a CSV with some data
        data_dir = tmp_path / "data" / "consolidated"
        data_dir.mkdir(parents=True)
        csv_file = data_dir / "all_listings.csv"
        csv_file.write_text(
            "listing_id,url,title,brand,model,year,price,currency,mileage_km,"
            "fuel_type,body_type,transmission,engine_capacity_cm3,engine_power_hp,"
            "color,location_city,location_region,seller_type,is_new,scraped_at,query_name\n"
            '1,https://x.com,BMW 320i,BMW,Seria 3,2020,50000.0,PLN,80000,'
            'Benzyna,,Manualna,1998,150,,Warszawa,Mazowieckie,private,,2026-04-14T12:00:00+00:00,test\n'
        )
        runner = CliRunner()
        result = runner.invoke(cli, ["stats", "--config", config])
        assert result.exit_code == 0
        assert "1" in result.output  # at least shows the count
