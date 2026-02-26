import logging
import sys


from data_integration_pipeline.settings import SERVICE_NAME, DEBUG, CODE_VERSION

# this doesns't work, there's something wrong with Helical's logger, it's consuming all logs
logger = logging.getLogger(SERVICE_NAME)

formatter = logging.Formatter(
    "%(asctime)s - %(name)s - %(levelname)s - %(pathname)s:%(lineno)d - %(message)s"
)

if DEBUG:
    logger.setLevel(logging.DEBUG)
else:
    logger.setLevel(logging.INFO)

local_handler = logging.StreamHandler(sys.stdout)
local_handler.setLevel(logging.DEBUG)
local_handler.setFormatter(formatter)

logger.addHandler(local_handler)


logger.info(f"Started {SERVICE_NAME}:{CODE_VERSION}")
