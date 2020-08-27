from pathlib import Path


BASE_DIR = Path(__file__).parents[2]
SRC_DIR = Path(__file__).parent
DATA_DIR = BASE_DIR / "data"
TEST_DIR = BASE_DIR / "tests"

RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
INTERIM_DIR = DATA_DIR / "interim"
EXTERNAL_DIR = DATA_DIR / "external"
REQUESTS_DIR = DATA_DIR / "requests"
ROUGHWORK_DIR = DATA_DIR / "roughwork"

FTEST_DATA_EXTERNAL = TEST_DIR / "functional" / "data" / "external"
FTEST_DATA_PROCESSED = TEST_DIR / "functional" / "data" / "processed"

UTEST_DATA_EXTRACT = TEST_DIR / "unit" / "extract" / "data"
UTEST_DATA_TRANSFORM = TEST_DIR / "unit" / "transform" / "data"
UTEST_DATA_UTILITIES = TEST_DIR / "unit" / "utilities" / "data"
