import time

import pytest

from otomotoskrap.client import OtomotoClient


class TestUserAgentRotation:
    def test_has_user_agents(self):
        client = OtomotoClient()
        assert len(client._user_agents) >= 10

    def test_random_ua_returns_string(self):
        client = OtomotoClient()
        ua = client._random_ua()
        assert isinstance(ua, str)
        assert "Mozilla" in ua

    def test_different_uas_over_calls(self):
        client = OtomotoClient()
        uas = {client._random_ua() for _ in range(50)}
        assert len(uas) > 1


class TestHeaders:
    def test_default_headers(self):
        client = OtomotoClient()
        headers = client._build_headers()
        assert "User-Agent" in headers
        assert headers["Accept-Language"].startswith("pl-PL")
        assert "Accept" in headers
        assert "Referer" in headers

    def test_referer_is_otomoto(self):
        client = OtomotoClient()
        headers = client._build_headers()
        assert "otomoto.pl" in headers["Referer"]


class TestDelay:
    def test_delay_within_range(self):
        client = OtomotoClient(delay_range=(0.01, 0.02))
        start = time.monotonic()
        client._wait()
        elapsed = time.monotonic() - start
        assert 0.01 <= elapsed < 0.1


class TestProxyConfig:
    def test_no_proxy_by_default(self):
        client = OtomotoClient()
        assert client._proxy is None

    def test_single_proxy(self):
        client = OtomotoClient(proxy="http://proxy:8080")
        assert client._proxy == "http://proxy:8080"
