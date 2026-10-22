"""Unit tests for src/download.py's archive-month probe (no network)."""

from datetime import date

import pytest

from src import config, download


@pytest.mark.parametrize("today, expected", [
    # 31 March: 30-day steps probed March, March again, January and skipped February
    (date(2027, 3, 31), [(2027, 3), (2027, 2), (2027, 1), (2026, 12)]),
    (date(2026, 10, 7), [(2026, 10), (2026, 9), (2026, 8), (2026, 7)]),
    (date(2027, 1, 1), [(2027, 1), (2026, 12), (2026, 11), (2026, 10)]),
])
def test_months_to_probe_steps_by_calendar_month(today, expected):
    assert download.months_to_probe(today) == expected


class _Head:
    def __init__(self, status_code):
        self.status_code = status_code


def test_latest_month_on_31_march_finds_february(monkeypatch):
    # Only February's archive is out: the old probe never asked for it.
    probed = []

    def head(url, **kwargs):
        probed.append(url)
        return _Head(200 if "MMSDM_2027_02/" in url else 404)
    monkeypatch.setattr(download.requests, "head", head)
    assert download.get_latest_available_month(date(2027, 3, 31)) == (2027, 2)
    assert [u.rsplit("/", 2)[-2] for u in probed] == ["MMSDM_2027_03", "MMSDM_2027_02"]
    assert all(u.startswith(config.MMSDM_BASE_URL) for u in probed)


class _Resp:
    def __init__(self, status_code, url="", history=()):
        self.status_code, self.url, self.history = status_code, url, list(history)


def _probe(monkeypatch, answers):
    """Run the probe on 31 March 2027; answers(url) -> response or exception."""
    monkeypatch.setattr(download.time, "sleep", lambda s: None)

    def head(url, **kwargs):
        r = answers(url)
        if isinstance(r, Exception):
            raise r
        return r
    monkeypatch.setattr(download.requests, "head", head)
    return download.get_latest_available_month(date(2027, 3, 31))


@pytest.mark.parametrize("status", [403, 500, 503])
def test_probe_does_not_step_back_on_a_blocked_or_server_error(monkeypatch, status):
    older_ok = lambda url: _Resp(200) if "MMSDM_2027_02/" in url else _Resp(status)
    assert _probe(monkeypatch, older_ok) is None


def test_probe_does_not_step_back_on_a_network_error(monkeypatch):
    err = download.requests.ConnectionError("down")
    assert _probe(monkeypatch, lambda url: err if "MMSDM_2027_03/" in url else _Resp(200)) is None


def test_probe_steps_back_on_a_redirect_to_the_404_page(monkeypatch):
    def answers(url):
        if "MMSDM_2027_03/" in url:
            return _Resp(403, "https://aemo.com.au/404", history=[_Resp(302, url)])
        return _Resp(200)
    assert _probe(monkeypatch, answers) == (2027, 2)
