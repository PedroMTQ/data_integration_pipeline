from importlib.metadata import version
import os

SERVICE_NAME = 'repo_template'
DEBUG = int(os.environ.get('DEBUG', '0'))
CODE_VERSION = version(SERVICE_NAME)

APP = os.path.dirname(__file__)
ROOT = os.path.abspath(os.path.dirname(os.path.dirname(os.path.dirname(__file__))))
DATA = os.path.join(ROOT, "data")
TEMP = os.path.join(ROOT, "tmp")
TESTS = os.path.join(ROOT, "tests")
CONFIGS = os.path.join(ROOT, 'confs')
