import os
import re
from pathlib import Path

import boto3
from botocore.client import Config
from retry import retry
from data_integration_pipeline.settings import (
    S3_ACCESS_KEY,
    S3_SECRET_ACCESS_KEY,
    S3_ENDPOINT_URL,
    S3_WRITE_RETRIES,
    S3_WRITE_RETRY_DELAY,
    S3_WRITE_RETRY_BACKOFF,
)

from data_integration_pipeline.io.logger import logger


class S3Client:
    def __init__(
        self,
        bucket_name: str,
        aws_access_key: str = S3_ACCESS_KEY,
        aws_secret_access_key: str = S3_SECRET_ACCESS_KEY,
        s3_endpoint_url: str = S3_ENDPOINT_URL,
    ):
        self.bucket_name = bucket_name
        self.endpoint_url = s3_endpoint_url
        config = Config(connect_timeout=5, retries={"max_attempts": 2})
        logger.debug(f"Trying to connect to {self.endpoint_url}")
        self.__client = boto3.client(
            "s3",
            aws_access_key_id=aws_access_key,
            aws_secret_access_key=aws_secret_access_key,
            endpoint_url=self.endpoint_url,
            config=config,
        )
        self.test_s3_connection()

    def test_s3_connection(self):
        try:
            self.__client.head_bucket(Bucket=self.bucket_name)
            logger.debug(f"Connected to S3 bucket: {self.bucket_name}")
            return True
        except Exception as e:
            logger.error(f"Failed to connect to S3 bucket: {self.bucket_name}")
            raise Exception(f"Error accessing bucket {self.bucket_name} due to: {e}") from e

    def get_files(self, prefix: str, file_name_pattern: str = None, match_on_s3_path: bool = False) -> list[str]:
        res = []
        try:
            response = self.__client.list_objects_v2(Bucket=self.bucket_name, Prefix=prefix)
        except Exception as e:
            raise e
        if file_name_pattern:
            regex_pattern = re.compile(file_name_pattern)
        else:
            regex_pattern = None
        for obj in response.get("Contents", []):
            s3_path = obj["Key"]
            if regex_pattern:
                file_name = Path(s3_path).name
                if regex_pattern.fullmatch(file_name) or (match_on_s3_path and regex_pattern.fullmatch(s3_path)):
                    res.append(s3_path)
                else:
                    logger.debug(f"File skipped: {s3_path}")
            else:
                res.append(s3_path)
        logger.debug(f"Files returned from {prefix}: {res}")
        return res

    def get_delta_tables(self, prefix: str) -> list[str]:
        """
        Returns a list of unique Delta table root paths (ending in .delta)
        found under the given prefix.
        """
        delta_roots = set()
        try:
            # Use a paginator in case you have thousands of files
            paginator = self.__client.get_paginator("list_objects_v2")
            pages = paginator.paginate(Bucket=self.bucket_name, Prefix=prefix)
            for page in pages:
                for obj in page.get("Contents", []):
                    s3_path = obj["Key"]
                    # Check if '.delta' is in the path
                    if ".delta" in s3_path:
                        # Logic: Split the path and find the segment ending in .delta
                        # e.g., 'silver/registry/business.delta/_delta_log/00.json'
                        parts = s3_path.split("/")
                        for i, part in enumerate(parts):
                            if part.endswith(".delta"):
                                # Reconstruct the path up to the .delta folder
                                root_path = "/".join(parts[: i + 1])
                                delta_roots.add(root_path)
                                break
        except Exception as e:
            logger.error(f"Error listing Delta tables: {e}")
            raise e
        res = sorted(list(delta_roots))
        logger.debug(f"Delta tables found under {prefix}: {res}")
        return res

    def delete_file(self, s3_path: str) -> bool:
        try:
            self.__client.delete_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as e:
            logger.exception(f"Failed to delete {s3_path} due to {e}")
            return False

    def move_file(self, current_path: str, new_path: str) -> str:
        try:
            self.__client.copy_object(
                Bucket=self.bucket_name,
                CopySource={"Bucket": self.bucket_name, "Key": current_path},
                Key=new_path,
            )
            self.__client.delete_object(Bucket=self.bucket_name, Key=current_path)
        except Exception as e:
            logger.exception(f"Failed to move {current_path} to {new_path} due to {e}")
        logger.info(f"Moved file from {current_path} to {new_path}")
        return new_path

    def download_file(self, s3_path: str, output_folder: str) -> str:
        """
        returns path of the downloaded file
        """
        Path(output_folder).mkdir(parents=True, exist_ok=True)
        file_name = Path(s3_path).name
        local_path = os.path.join(output_folder, file_name)
        self.__client.download_file(self.bucket_name, s3_path, local_path)
        logger.debug(f"Downloaded {s3_path} to {output_folder}")
        return local_path

    def upload_file(self, local_path: str, s3_path: str) -> bool:
        """
        Uploads a local file to S3 at the given s3_path.
        """
        try:
            self.__client.upload_file(Filename=local_path, Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as e:
            logger.error(f"Failed to upload {local_path} to {s3_path} due to {e}")
            return False

    def read_file(self, s3_path: str) -> bytes:
        return self.__client.get_object(Bucket=self.bucket_name, Key=s3_path)["Body"].read()

    def file_exists(self, s3_path: str) -> bool:
        try:
            self.__client.get_object(Bucket=self.bucket_name, Key=s3_path)
            return True
        except Exception as _:
            return False

    @retry(tries=S3_WRITE_RETRIES, delay=S3_WRITE_RETRY_DELAY, backoff=S3_WRITE_RETRY_BACKOFF, logger=logger)
    def get_object(self, object_name: str):
        """Generate a presigned URL to access the object."""
        try:
            return self.__client.get_object(self.bucket_name, object_name)
        except Exception as e:
            logger.error(f"Could not find object {object_name} due to {e}")


if __name__ == "__main__":
    client = S3Client(bucket_name="data")
    # print(client.file_exists('boxes/output/bounding_box_01976a1225ca7e32a2daad543cb4391e.jsonl'))
    # print(client.file_exists("test"))
    print(client.get_delta_tables("silver"))
    # print(client.get_files(FIELDS_FOLDER_OUTPUT, file_name_pattern='fields/input/01976dbcbdb77dc4b9b61ba545503b77/fields_2025-06-04-BATCH_2.jsonl', match_on_s3_path=True))
