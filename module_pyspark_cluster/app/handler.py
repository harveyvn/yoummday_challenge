from minio import Minio
from minio.error import S3Error
import os
from utils.logging import log


def upload_file_to_minio(local_file_path, bucket_name, object_name):
    # Connect to MinIO server
    client = Minio(
        endpoint=os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY", "admin"),
        secret_key=os.getenv("MINIO_SECRET_KEY", "password"),
        secure=False
    )

    # Create bucket if not exists
    found = client.bucket_exists(bucket_name)
    if not found:
        client.make_bucket(bucket_name)
        log.info(f"Bucket '{bucket_name}' created.")
    else:
        log.info(f"Bucket '{bucket_name}' already exists.")

    # Upload the file
    try:
        client.fput_object(bucket_name, object_name, local_file_path)
        log.info(f"File '{local_file_path}' uploaded as '{object_name}' in bucket '{bucket_name}'.")
    except S3Error as err:
        log.info(f"Failed to upload: {err}")


if __name__ == "__main__":
    local_file = "static_files/dataset.txt"  # adjust path to your static file
    bucket = "ingestion-bucket"
    object_name = "dataset.txt"  # name inside the bucket

    upload_file_to_minio(local_file, bucket, object_name)
