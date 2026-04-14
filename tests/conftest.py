# tests/conftest.py
from pathlib import Path

import pytest


@pytest.fixture
def fixtures_dir():
    return Path(__file__).parent / "fixtures"


@pytest.fixture
def sample_html(fixtures_dir):
    return (fixtures_dir / "search_page.html").read_text()
