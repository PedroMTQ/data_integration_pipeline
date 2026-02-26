from importlib.metadata import version
import os
import tomllib

os.environ["PYDANTIC_ERRORS_INCLUDE_URL"] = "0"

DEBUG = int(os.environ.get("DEBUG", "0"))

APP = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
PYPROJECT_PATH = os.path.join(ROOT, "pyproject.toml")
DATA = os.path.join(ROOT, "data")
TEMP = os.path.join(ROOT, "tmp")
TESTS = os.path.join(ROOT, "tests")
TESTS_DATA = os.path.join(TESTS, "data")
CONFIGS = os.path.join(ROOT, "confs")

with open(PYPROJECT_PATH, "rb") as f:
    data = tomllib.load(f)
    SERVICE_NAME = data["project"]["name"]

CODE_VERSION = version(SERVICE_NAME)
SRC_DATA = os.path.join(APP, "data")


MIN_LENGTH_ADDRESS_1 = int(os.getenv("MIN_LENGTH_ADDRESS_1", "3"))


ARCHIVE_DATA_FOLDER = "archive"
BRONZE_DATA_FOLDER = "bronze"
SILVER_DATA_FOLDER = "silver"
ERRORS_DATA_FOLDER = "errors"
GOLD_DATA_FOLDER = "gold"

MAX_RETRIES = int(os.environ.get("MAX_RETRIES", "10"))


POSTGRES_HOST = os.environ.get("POSTGRES_HOST")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_USER = os.environ.get("POSTGRES_USER")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_DATABASE = os.environ.get("POSTGRES_DATABASE")
POSTGRES_STATIC_DBS = ["postgres"]
POSTGRES_CLIENT_BATCH_SIZE = int(os.environ.get("POSTGRES_CLIENT_BATCH_SIZE", "10000"))

CONNECTION_TIMEOUT = int(os.environ.get("CONNECTION_TIMEOUT", "10"))
STATEMENT_TIMEOUT = int(os.environ.get("STATEMENT_TIMEOUT", "60"))


# we could create and use access keys, but for now we keep it like this
S3_ACCESS_KEY = os.getenv("S3_ACCESS_KEY")
S3_SECRET_ACCESS_KEY = os.getenv("S3_SECRET_ACCESS_KEY")
S3_HOST = os.getenv("S3_HOST")
S3_PORT = os.getenv("S3_PORT")
S3_ENDPOINT_URL = f"http://{S3_HOST}:{S3_PORT}"

S3_WRITE_RETRIES = int(os.getenv("S3_WRITE_RETRIES", "5"))
S3_WRITE_RETRY_DELAY = int(os.getenv("S3_WRITE_RETRY_DELAY", "5"))
S3_WRITE_RETRY_BACKOFF = int(os.getenv("S3_WRITE_RETRY_BACKOFF", "5"))


DELTA_VAULT_BUCKET = os.getenv("DELTA_VAULT_BUCKET")
DATA_BUCKET = os.getenv("DATA_BUCKET")


NATURAL_KEY_COLUMN = os.getenv("NATURAL_KEY_COLUMN", "nk")
SOURCE_KEY_COLUMN = os.getenv("SOURCE_KEY_COLUMN", "rsrc")
PRIMARY_KEY_COLUMN = os.getenv("PRIMARY_KEY_COLUMN", "hk")
HASH_DIFF_COLUMN = os.getenv("HASH_DIFF_COLUMN", "hdiff")
LDTS_COLUMN = os.getenv("LDTS_COLUMN", "ldts")
PAYLOAD_KEY = os.getenv("PAYLOAD_KEY", "payload")
FOREIGN_KEY_COLUMN = os.getenv("FOREIGN_KEY_COLUMN", "fk")


DELTA_TABLE_URI = f"s3a://{DELTA_VAULT_BUCKET}"
STORAGE_OPTIONS = {
    "AWS_ACCESS_KEY": S3_ACCESS_KEY,
    "AWS_SECRET_ACCESS_KEY": S3_SECRET_ACCESS_KEY,
    "AWS_ENDPOINT_URL": f"http://{S3_HOST}:{S3_PORT}",
    "AWS_ALLOW_HTTP": "true",
}
DELTA_CLIENT_BATCH_SIZE = int(os.environ.get("DELTA_CLIENT_BATCH_SIZE", "10000"))
