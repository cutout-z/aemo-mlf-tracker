"""Configuration for AEMO MLF Tracker."""

# NEM regions
REGIONS = ["NSW1", "QLD1", "VIC1", "SA1", "TAS1"]

REGION_NAMES = {
    "NSW1": "NSW",
    "QLD1": "QLD",
    "VIC1": "VIC",
    "SA1": "SA",
    "TAS1": "TAS",
}

# Financial years to track (start year of FY, e.g. 2015 = FY 2015-16)
FY_START = 2015
FY_END = 2025  # FY 2025-26

# MMSDM archive URL template for DUDETAILSUMMARY
MMSDM_BASE_URL = "https://nemweb.com.au/Data_Archive/Wholesale_Electricity/MMSDM/"
DUDETAILSUMMARY_URL_TEMPLATE = (
    MMSDM_BASE_URL
    + "{year:04d}/MMSDM_{year:04d}_{month:02d}/"
    "MMSDM_Historical_Data_SQLLoader/DATA/"
    "PUBLIC_ARCHIVE%23DUDETAILSUMMARY%23FILE01%23{year:04d}{month:02d}010000.zip"
)

# Fuel type categories for grouping (maps "Fuel Source - Primary" values)
FUEL_TYPE_MAP = {
    "Solar": "Solar",
    "Wind": "Wind",
    "Hydro": "Hydro",
    "Battery Storage": "Battery",
    "Fossil": "Fossil",
    "Renewable/ Biomass / Waste": "Other Renewable",
    "Renewable/ Biomass / Waste and Fossil": "Other Renewable",
    "-": "Other",
}

# Paths (relative to project root)
DATA_DIR = "data"
OUTPUT_DIR = "outputs"
SUMMARY_CSV = "outputs/summary.csv"
CACHE_FILE = "data/dudetailsummary.feather"
GENERATOR_CACHE = "data/generators.feather"

# Network retry settings
MAX_RETRIES = 3
RETRY_BACKOFF = 5  # seconds
