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
