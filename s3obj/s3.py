from pathlib import Path
from typing import Optional

import boto3
from botocore.exceptions import ClientError
import os

from loguru import logger
from botocore.config import Config as Boto3Config

from s3obj.utils import get_extension


class S3Boto3:
    """
    A wrapper class around the boto3 s3 client
    """

    def __init__(
            self,
            endpoint_url: str = None,
            config: Boto3Config = Boto3Config(
                retries={"max_attempts": 10, "mode": "adaptive"}
            ),
    ):
        self.s3_resource = boto3.resource(
            "s3", endpoint_url=endpoint_url, config=config
        )
        self.s3_client = boto3.client("s3", endpoint_url=endpoint_url, config=config)

    def get_header(self, bucket: str, prefix: str):
        header = self.s3_client.head_object(Bucket=bucket, Key=prefix)
        return {k: v for k, v in header.items() if
                k in {"LastModified", "ContentLength", "ETag", "VersionId", "ContentType", "Metadata"}}

    def check_exists(self, bucket: str, prefix: str):
        """
        Check if object exists in s3
        Args:
            bucket:
            prefix:

        Returns:

        """
        try:
            self.get_header(bucket, prefix)
            return True
        except ClientError:
            return False

    def get_s3_path(self, bucket: str, prefix: str):
        """
        get the s3 path
        Args:
            bucket:
            prefix:

        Returns:

        """
        return f"s3://{bucket}/{prefix}"

    def upload_file(self, file_name, bucket, prefix=None, overwrite: bool = False):
        """
        Upload a file to an S3 bucket
        Args:
            file_name:
            bucket:
            prefix: If S3 prefix was not specified, use file_name
            overwrite:

        Returns:

        """

        if prefix is None:
            prefix = os.path.basename(file_name)

        if not overwrite and self.check_exists(bucket, prefix):
            logger.info(
                f"[Upload] {self.get_s3_path(bucket, prefix)} exists -- skipping"
            )
            return True

        try:
            response = self.s3_client.upload_file(file_name, bucket, prefix)
            logger.info(f"[Uploaded] {self.get_s3_path(bucket, prefix)}")
        except ClientError as e:
            logger.error(e)
            return False
        return True

    def download_file(
            self, bucket: str, prefix: str, target: str, overwrite: bool = False
    ):
        """
        Download file from s3
        Args:
            bucket:
            prefix:
            target: target path
            overwrite:

        Returns:

        """

        target = Path(target)
        if target.exists() and not overwrite:
            logger.info(f"[already exists] {target}, skipping download.")
            return
        if not target.parent.exists():
            target.parent.mkdir(parents=True, exist_ok=True)

        logger.info(f"[Downloading] {self.get_s3_path(bucket, prefix)} -> {target}")
        self.s3_resource.Bucket(bucket).download_file(prefix, str(target))


class S3Object:
    """
    A class to represent an s3 object to make it easier to upload and download
    """

    def __init__(
            self,
            bucket: str,
            prefix: str,
            key: Optional[str] = None,
            local_path: Optional[str] = None,
            base_dir: str = "/tmp",
            s3_client: Optional[S3Boto3] = None,
            **kwargs,
    ):
        """

        Args:
            bucket: s3 bucket
            prefix: s3 prefix
            key: a key that represents the objects
            local_path: a specific local path to use. If None it is derived from prefix and base_dir
            base_dir: a base directory to use locally
            s3_client: the s3 client
            **kwargs:
        """
        self.bucket = bucket
        self.prefix = prefix
        self.base_dir = base_dir
        self.key = key
        self._name = None
        self._ext = None
        self._local_path = local_path
        self._header = None
        self.s3_client = s3_client or S3Boto3()

    def __repr__(self):
        return f"{self.__class__.__name__}(name={self.name}, s3_path={self.s3_path}, local_path={self.local_path})"

    @classmethod
    def from_s3_path(
            cls,
            s3_path: str,
            base_dir: str = "/tmp",
            key: Optional[str] = None,
            local_path: Optional[str] = None,
            **kwargs,
    ):
        path_parts = s3_path.replace("s3://", "").split("/")
        bucket = path_parts.pop(0)
        prefix = "/".join(path_parts)
        return cls(
            bucket=bucket,
            prefix=prefix,
            base_dir=base_dir,
            key=key,
            local_path=local_path,
            **kwargs,
        )

    @property
    def header(self):
        if self._header is None:
            self._header = self.s3_client.get_header(self.bucket, self.prefix)
        return self._header

    @classmethod
    def from_local_path(
            cls,
            local_path: str,
            base_dir: str = "/tmp",
            key: Optional[str] = None,
            **kwargs,
    ):
        if base_dir not in local_path:
            raise Exception(f"base_dir {base_dir} not part of {local_path}")
        return cls(
            bucket=kwargs.pop("bucket") if "bucket" in kwargs else "local",
            prefix=kwargs.pop("prefix") if "prefix" in kwargs else str(Path(local_path).relative_to(base_dir)),
            base_dir=base_dir,
            key=key,
            local_path=local_path,
            **kwargs,
        )

    @property
    def local_path(self):
        """
        use provided local path; otherwise, use from prefix
        """
        return self._local_path or str(Path(f"{self.base_dir}/{self.prefix}"))

    @property
    def s3_path(self) -> str:
        """

        Returns: full s3 path

        """
        return f"s3://{self.bucket}/{self.prefix}"

    @property
    def extension(self) -> str:
        """

        Returns: file extension

        """
        if self._ext is None:
            self._ext = get_extension(self.prefix)

        return self._ext

    @property
    def name(self):
        """

        Returns: name of object

        """
        if self._name is None:
            path = Path(self.prefix)
            self._name = path.name.replace(self.extension, "")
        return self._name

    @property
    def basename(self):
        """

        Returns: basename from prefix

        """
        return Path(self.prefix).name

    def download(self, overwrite: bool = False):
        """
        Download an object from s3
        Args:
            overwrite: Whether to overwrite if it already exists

        Returns: None

        """

        self.s3_client.download_file(bucket=self.bucket,
                                     prefix=self.prefix,
                                     target=self.local_path, overwrite=overwrite)

    def upload(self, overwrite: bool = False) -> None:
        """
        Upload an object to s3
        Args:
            overwrite: Whether to overwrite if it already exists

        Returns: None

        """
        self.s3_client.upload_file(file_name=self.local_path, bucket=self.bucket, prefix=self.prefix,
                                   overwrite=overwrite)

    def exists_local(self):
        """
        Check if object exists locally
        Returns: True if exists

        """
        return os.path.exists(self.local_path)

    def exists_remote(self):
        """
        Check if object exists on s3
        Returns: True if exists

        """
        return self.s3_client.check_exists(self.bucket, self.prefix)

    def delete(self):
        """
        Delete an object locally
        Returns:

        """
        try:
            os.remove(self.local_path)
            logger.info(f"[Removed] {self.local_path}")
        except Exception as e:
            logger.warning(e)
