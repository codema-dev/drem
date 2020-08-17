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

TEST_DATA_EXTERNAL = TEST_DIR / "extract" / "data"
TEST_DATA_TRANSFORM = TEST_DIR / "transform" / "data"
