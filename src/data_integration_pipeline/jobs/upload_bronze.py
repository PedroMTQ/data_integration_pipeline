from data_integration_pipeline.io.s3_client import S3Client
from data_integration_pipeline.settings import TESTS_DATA, BRONZE_DATA_FOLDER
import os
from pathlib import Path
from data_integration_pipeline.io.logger import logger


class UploadBronzeJob:
    def __init__(self):
        self.s3_client = S3Client()

    def run(self):
        for file in os.listdir(TESTS_DATA):
            local_file_path = os.path.join(TESTS_DATA, file)
            if not os.path.isfile(local_file_path):
                continue
            parent_folder = Path(file).stem
            s3_path = os.path.join(BRONZE_DATA_FOLDER, parent_folder, file)
            upload_status = self.s3_client.upload_file(local_path=local_file_path, s3_path=s3_path)
            if upload_status:
                logger.info(f"Uploaded {local_file_path} to {s3_path}")
            else:
                logger.warning(f"Failed to upload {local_file_path} to {s3_path}")


if __name__ == "__main__":
    job = UploadBronzeJob()
    job.run()
