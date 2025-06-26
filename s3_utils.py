import os
import uuid
from datetime import datetime
import boto3
from dotenv import load_dotenv
from botocore.exceptions import ClientError


load_dotenv()

AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
AWS_REGION = os.getenv("AWS_REGION")
BUCKET_NAME = os.getenv("S3_BUCKET")

s3 = boto3.client(
    's3',
    region_name=AWS_REGION,
    aws_access_key_id=AWS_ACCESS_KEY,
    aws_secret_access_key=AWS_SECRET_KEY
)

def ensure_bucket_exists(bucket_name):
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"✅ Bucket '{bucket_name}' already exists.")
    except ClientError as e:
        if e.response['Error']['Code'] == '404':
            print(f"📦 Bucket '{bucket_name}' not found. Creating it...")
            s3.create_bucket(
                Bucket=bucket_name,
                CreateBucketConfiguration={'LocationConstraint': AWS_REGION}
            )
            set_lifecycle_policy(bucket_name)
        else:
            raise e


def set_lifecycle_policy(bucket_name):
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'ArchiveFolderToGlacier',
                'Filter': {},
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': 30,
                        'StorageClass': 'GLACIER'
                    }
                ],
                'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7}
            }
        ]
    }

    try:
        s3.put_bucket_lifecycle_configuration(
            Bucket=bucket_name,
            LifecycleConfiguration=lifecycle_config
        )
        print(f"✅ Lifecycle rule set to move all bucket content to Glacier after 30 days.")
    except ClientError as e:
        print(f"❌ Error setting lifecycle configuration: {e}")

def upload_parquet_from_path(local_path, bucket_name):
    today_folder = datetime.now().strftime("%b%d%Y").lower()
    base_key = os.path.basename(local_path)
    key = f"{today_folder}/{base_key}"

    with open(local_path, "rb") as f:
        s3.upload_fileobj(f, bucket_name, key)

    print(f"✅ Uploaded archive to s3://{bucket_name}/{key}")


def convert_uuids_to_str(df):
    for col in df.columns:
        if df[col].apply(lambda x: isinstance(x, uuid.UUID)).any():
            df[col] = df[col].astype(str)
    return df

