import logging
import os
from datetime import datetime, timezone

logger = logging.getLogger(__name__)


def upload_to_s3(config, local_filepath):
    """Upload the report CSV to S3. Falls back to local-only if credentials are unavailable."""
    bucket = config["s3"]["bucket"]
    prefix = config["s3"]["prefix"]
    region = config["s3"]["region"]

    filename = os.path.basename(local_filepath)
    s3_key = f"{prefix}/{filename}"

    try:
        import boto3
        from botocore.exceptions import NoCredentialsError, ClientError

        s3_client = boto3.client("s3", region_name=region)
        s3_client.upload_file(local_filepath, bucket, s3_key)
        s3_uri = f"s3://{bucket}/{s3_key}"
        logger.info(f"Report uploaded to S3: {s3_uri}")
        return s3_uri

    except (NoCredentialsError, ClientError) as e:
        logger.warning(f"S3 upload failed ({e}). Report saved locally: {local_filepath}")
        return None

    except ImportError:
        logger.warning("boto3 not installed. Report saved locally: {local_filepath}")
        return None
