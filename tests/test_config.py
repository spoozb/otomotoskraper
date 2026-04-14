import pytest
import yaml

from otomotoskrap.config import load_config
from otomotoskrap.models import AppConfig


def test_load_valid_config(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "queries": [
                    {
                        "name": "bmw",
                        "url": "https://www.otomoto.pl/osobowe/bmw",
                        "max_pages": 10,
                    }
                ],
                "settings": {
                    "delay_range": [1, 3],
                    "max_retries": 2,
                    "output_dir": "./out",
                },
            }
        )
    )
    config = load_config(str(config_file))
    assert isinstance(config, AppConfig)
    assert config.queries[0].name == "bmw"
    assert config.settings.delay_range == [1, 3]


def test_load_config_defaults(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text(
        yaml.dump(
            {
                "queries": [
                    {"name": "test", "url": "https://www.otomoto.pl/osobowe"},
                ],
            }
        )
    )
    config = load_config(str(config_file))
    assert config.settings.max_retries == 3
    assert config.settings.output_dir == "./data"


def test_load_config_missing_file():
    with pytest.raises(FileNotFoundError):
        load_config("/nonexistent/config.yaml")


def test_load_config_invalid_yaml(tmp_path):
    config_file = tmp_path / "config.yaml"
    config_file.write_text("queries: not_a_list")
    with pytest.raises(Exception):
        load_config(str(config_file))
