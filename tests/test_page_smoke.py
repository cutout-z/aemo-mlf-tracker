"""Rendered-page smoke: load index.html in headless chromium, offline, with the data the repo commits.

No server and no port: page_harness routes http://aemo.test/ to this checkout, PapaParse to a vendored
copy, SheetJS to a no-op stub, nav.js to an empty script, and aborts (and records) every other request.
"""

import csv
import re

import pytest

try:
    import playwright  # noqa: F401
except ImportError as exc:  # a collection ERROR, which the merge gate counts; never a silent skip
    raise RuntimeError(
        "these smoke tests need Playwright (pinned in requirements-dev.txt); run the suite with "
        "/opt/anaconda3/bin/python3 (playwright 1.58.0 + chromium) or: pip install -r requirements-dev.txt "
        "&& playwright install chromium"
    ) from exc

from page_harness import ROOT, assert_clean, loaded_page  # noqa: E402

READY = "document.querySelectorAll('#genTbody tr').length > 0"


def _csv_rows():
    with open(ROOT / "outputs" / "summary.csv", newline="") as f:
        return list(csv.DictReader(f))


def _shown(page):
    """(rows in the two tables, the number the 'N DUIDs shown' badge claims)."""
    rows = page.locator("#genTbody tr").count() + page.locator("#battTbody tr").count()
    return rows, int(re.match(r"(\d+)", page.locator("#rowCount").inner_text()).group(1))


def test_page_renders_the_committed_data_with_no_errors():
    committed = len(_csv_rows())
    with loaded_page(wait_for=READY) as (page, log):
        assert_clean(log)
        # stat tiles: numbers, none a skeleton; the file total matches the committed CSV
        stats = page.locator("#stats").inner_text()
        assert page.locator("#stats .skeleton").count() == 0
        assert re.search(r"\b0\.\d{4}\b", stats), stats
        assert f"of {committed} in the file" in stats, stats
        # both tables are filled, and the badge agrees with what is on screen
        rows, claimed = _shown(page)
        assert page.locator("#genTbody tr").count() > 100 and page.locator("#battTbody tr").count() > 10
        assert rows == claimed and 0 < claimed <= committed
        assert re.search(r"\d\.\d{4}", page.locator("#genTbody tr").first.inner_text())
        for sel in ("#stats", "#controls", "#genTable", "#battTable", "#footer", "#runStatus"):
            box = page.locator(sel).bounding_box()
            assert box and box["height"] > 0 and page.locator(sel).is_visible(), sel
        assert page.locator("#loading").is_hidden()
        assert page.locator("#genState").is_hidden()
        assert f"{committed} DUIDs tracked" in page.locator("#footer").inner_text()
        assert "MMSDM archive" in page.locator("#runStatus").inner_text()


def test_region_filter_and_selection_export_use_the_stubbed_sheetjs():
    with loaded_page(wait_for=READY) as (page, log):
        _, all_regions = _shown(page)
        page.locator("#tabs button", has_text="TAS").click()
        page.wait_for_function(f"document.querySelectorAll('#genTbody tr').length < {all_regions}")
        rows, claimed = _shown(page)
        assert rows == claimed < all_regions
        page.locator("#genTbody label.cbl").first.click()
        assert page.locator("#selCount").inner_text().startswith("1 asset selected")
        assert page.locator("#exportSelected").is_enabled()
        page.locator("#exportSelected").click()
        assert page.evaluate("window.__xlsxWrites") == 1  # the click reached XLSX.writeFile (the stub)
        assert_clean(log)


def test_page_renders_at_phone_width():
    with loaded_page(width=400, height=800, wait_for=READY) as (page, log):
        assert_clean(log)
        assert page.locator("#genTbody tr").count() > 0
        assert page.locator("#stats").is_visible()
