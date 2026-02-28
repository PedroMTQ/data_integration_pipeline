from importlib.metadata import version
import os
import tomllib

os.environ["PYDANTIC_ERRORS_INCLUDE_URL"] = "0"

DEBUG = int(os.getenv("DEBUG", "0"))

APP = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
PYPROJECT_PATH = os.path.join(ROOT, "pyproject.toml")
DATA = os.path.join(ROOT, "data")
TEMP = os.path.join(ROOT, "tmp")
TESTS = os.path.join(ROOT, "tests")
TESTS_DATA = os.path.join(TESTS, "data")
CONFIG = os.path.join(ROOT, "config")
ER_SPEC_PATH = os.path.join(CONFIG, "er_spec.yaml")
ER_TEMP = os.path.join(TEMP, "er")

with open(PYPROJECT_PATH, "rb") as f:
    data = tomllib.load(f)
    SERVICE_NAME = data["project"]["name"]

CODE_VERSION = version(SERVICE_NAME)
SRC_DATA = os.path.join(APP, "data")


MIN_LENGTH_ADDRESS_1 = int(os.getenv("MIN_LENGTH_ADDRESS_1", "3"))


ARCHIVE_DATA_FOLDER = "archive"
BRONZE_DATA_FOLDER = "bronze"
ARCHIVE_DATA_FOLDER = "archive"
SILVER_DATA_FOLDER = "silver"
ENTITY_RESOLUTION_DATA_FOLDER = "entity_resolution"
ERRORS_DATA_FOLDER = "errors"
PROCESSING_ERRORS_DATA_FOLDER = os.path.join(ERRORS_DATA_FOLDER, "processsing")
LOADING_ERRORS_DATA_FOLDER = os.path.join(ERRORS_DATA_FOLDER, "loading")
GOLD_DATA_FOLDER = "gold"

LINKS_FILE_NAME = "links.parquet"
LINKS_METADATA_FILE_NAME = "metadata.json"
LINKS_MODEL_FILE_NAME = "model.json"

MAX_RETRIES = int(os.getenv("MAX_RETRIES", "10"))


POSTGRES_HOST = os.getenv("POSTGRES_HOST")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_USER = os.getenv("POSTGRES_USER")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.getenv("POSTGRES_DATABASE")
POSTGRES_STATIC_DBS = ["postgres"]
POSTGRES_CLIENT_BATCH_SIZE = int(os.getenv("POSTGRES_CLIENT_BATCH_SIZE", "10000"))

CONNECTION_TIMEOUT = int(os.getenv("CONNECTION_TIMEOUT", "10"))
STATEMENT_TIMEOUT = int(os.getenv("STATEMENT_TIMEOUT", "60"))


# we could create and use access keys, but for now we keep it like this
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_HOST = os.getenv("S3_HOST")
S3_PORT = os.getenv("S3_PORT")
S3_ENDPOINT_URL = f"http://{S3_HOST}:{S3_PORT}"

S3_WRITE_RETRIES = int(os.getenv("S3_WRITE_RETRIES", "5"))
S3_WRITE_RETRY_DELAY = int(os.getenv("S3_WRITE_RETRY_DELAY", "5"))
S3_WRITE_RETRY_BACKOFF = int(os.getenv("S3_WRITE_RETRY_BACKOFF", "5"))


AUDIT_TOTAL_ROWS = int(os.getenv("AUDIT_TOTAL_ROWS", "1000"))
DATA_BUCKET = os.getenv("DATA_BUCKET")
UNKNOWN_PARTITION_STR = "UNKNOWN"

NATURAL_KEY_COLUMN = os.getenv("NATURAL_KEY_COLUMN", "nk")
SOURCE_KEY_COLUMN = os.getenv("SOURCE_KEY_COLUMN", "rsrc")
PRIMARY_KEY_COLUMN = os.getenv("PRIMARY_KEY_COLUMN", "hk")
HASH_DIFF_COLUMN = os.getenv("HASH_DIFF_COLUMN", "hdiff")
LDTS_COLUMN = os.getenv("LDTS_COLUMN", "ldts")
PAYLOAD_KEY = os.getenv("PAYLOAD_KEY", "payload")
FOREIGN_KEY_COLUMN = os.getenv("FOREIGN_KEY_COLUMN", "fk")


DELTA_TABLE_URI = f"s3a://{DATA_BUCKET}"
STORAGE_OPTIONS = {
    "aws_access_key_id": S3_ACCESS_KEY,
    "aws_secret_access_key": S3_SECRET_ACCESS_KEY,
    "endpoint_url": f"http://{S3_HOST}:{S3_PORT}",
    "allow_http": "true",
}


DELTA_CLIENT_BATCH_SIZE = int(os.getenv("DELTA_CLIENT_BATCH_SIZE", "10000"))


SPLINK_INFERENCE_PREDICT_THRESHOLD = float(os.getenv("SPLINK_INFERENCE_PREDICT_THRESHOLD", "0.01"))
SPLINK_CLUSTERING_THRESHOLD = float(os.getenv("SPLINK_CLUSTERING_THRESHOLD", "0.9"))
