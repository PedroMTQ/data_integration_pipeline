from importlib.metadata import version
import os
import tomllib

os.environ['PYDANTIC_ERRORS_INCLUDE_URL'] = '0'

DEBUG = int(os.getenv('DEBUG', '0'))

APP = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
PYPROJECT_PATH = os.path.join(ROOT, 'pyproject.toml')
DATA = os.path.join(ROOT, 'data')
TEMP = os.path.join(ROOT, 'tmp')
TESTS = os.path.join(ROOT, 'tests')
TESTS_DATA = os.path.join(TESTS, 'data')
CONFIG = os.path.join(ROOT, 'config')

with open(PYPROJECT_PATH, 'rb') as f:
    data = tomllib.load(f)
    SERVICE_NAME = data['project']['name']

CODE_VERSION = version(SERVICE_NAME)
SRC_DATA = os.path.join(APP, 'data')


ARCHIVE_DATA_FOLDER = 'archive'
BRONZE_DATA_FOLDER = 'bronze'
SILVER_DATA_FOLDER = 'silver'
ENTITY_RESOLUTION_DATA_FOLDER = 'entity_resolution'
DATA_MART_DATA_FOLDER = 'data_mart'
ERRORS_DATA_FOLDER = 'errors'
PROCESSING_ERRORS_DATA_FOLDER = os.path.join(ERRORS_DATA_FOLDER, 'processing')

DELTA_TABLE_SUFFIX = '.delta'
PARQUET_TABLE_SUFFIX = '.parquet'
LINKS_FILE_NAME = f'links{PARQUET_TABLE_SUFFIX}'
LINKS_METADATA_FILE_NAME = 'metadata.json'
LINKS_MODEL_FILE_NAME = 'model.json'

COMPOSITE_ID_STR = 'composite_id'
CLUSTER_ID_STR = 'cluster_id'
DATA_SOURCE_STR = 'data_source'

S3_ACCESS_KEY = os.getenv('S3_ACCESS_KEY')
S3_SECRET_ACCESS_KEY = os.getenv('S3_SECRET_ACCESS_KEY')
S3_HOST = os.getenv('S3_HOST')
S3_PORT = os.getenv('S3_PORT')
S3_ENDPOINT_URL = f'http://{S3_HOST}:{S3_PORT}'

S3_WRITE_RETRIES = int(os.getenv('S3_WRITE_RETRIES', '5'))
S3_WRITE_RETRY_DELAY = int(os.getenv('S3_WRITE_RETRY_DELAY', '5'))
S3_WRITE_RETRY_BACKOFF = int(os.getenv('S3_WRITE_RETRY_BACKOFF', '5'))


AUDIT_TOTAL_ROWS = int(os.getenv('AUDIT_TOTAL_ROWS', '1000'))
DATA_BUCKET = os.getenv('DATA_BUCKET')
UNKNOWN_PARTITION_STR = 'UNKNOWN'


HASH_DIFF_COLUMN = os.getenv('HASH_DIFF_COLUMN', 'hdiff')
LDTS_COLUMN = os.getenv('LDTS_COLUMN', 'ldts')


DELTA_TABLE_URI = f's3a://{DATA_BUCKET}'
STORAGE_OPTIONS = {
    'aws_access_key_id': S3_ACCESS_KEY,
    'aws_secret_access_key': S3_SECRET_ACCESS_KEY,
    'endpoint_url': f'http://{S3_HOST}:{S3_PORT}',
    'allow_http': 'true',
}


DELTA_CLIENT_BATCH_SIZE = int(os.getenv('DELTA_CLIENT_BATCH_SIZE', '10000'))


SPLINK_INFERENCE_PREDICT_THRESHOLD = float(os.getenv('SPLINK_INFERENCE_PREDICT_THRESHOLD', '0.01'))
SPLINK_CLUSTERING_THRESHOLD = float(os.getenv('SPLINK_CLUSTERING_THRESHOLD', '0.9'))
COMPOSITE_KEY_SEP = '-__-'
