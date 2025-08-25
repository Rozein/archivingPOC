import os
import aioboto3
from io import BytesIO

from botocore.exceptions import ClientError
from dotenv import load_dotenv
import asyncio
import asyncpg


load_dotenv()

AWS_REGION = os.getenv("AWS_REGION")
AWS_ACCESS_KEY = os.getenv("AWS_ACCESS_KEY_ID")
AWS_SECRET_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")

session = aioboto3.Session()
async def ensure_bucket_exists(bucket):
    async with session.client("s3", region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET_KEY) as s3:
        try:
            await s3.head_bucket(Bucket=bucket)
            print(f"✅ Bucket '{bucket}' already exists.")
        except s3.exceptions.ClientError as e:
            if e.response['Error']['Code'] == '404':
                await s3.create_bucket(Bucket=bucket, CreateBucketConfiguration={'LocationConstraint': AWS_REGION})
                print(f"📦 Created bucket: {bucket}")

                await set_lifecycle_policy(bucket)
            else:
                raise

async def set_lifecycle_policy(bucket):
    lifecycle_config = {
        'Rules': [
            {
                'ID': 'ArchiveFolderToGlacier',
                'Filter': {},
                'Status': 'Enabled',
                'Transitions': [
                    {
                        'Days': 1,
                        'StorageClass': 'GLACIER'
                    }
                ],
                'AbortIncompleteMultipartUpload': {'DaysAfterInitiation': 7}
            }
        ]
    }
    async with session.client("s3", region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET_KEY) as s3:
        await s3.put_bucket_lifecycle_configuration(
            Bucket=bucket,
            LifecycleConfiguration=lifecycle_config
        )
        print("✅ Lifecycle policy applied.")

async def upload_parquet_from_dataframe(df, bucket, key):
    buffer = BytesIO()
    loop = asyncio.get_running_loop()
    converted_df = convert_uuids_to_str(df)
    await loop.run_in_executor(None, lambda: converted_df.to_parquet(buffer, index=False))
    buffer.seek(0)

    async with session.client("s3", region_name=AWS_REGION,
                                aws_access_key_id=AWS_ACCESS_KEY,
                                aws_secret_access_key=AWS_SECRET_KEY) as s3:
        await s3.upload_fileobj(buffer, bucket, key)
    print(f"✅ Uploaded to s3://{bucket}/{key}")

async def copy_s3_files_from_keys(keys, archive_bucket, archive_prefix):
    SOURCE_BUCKET = os.getenv("S3_SOURCE_BUCKET")
    SOURCE_SUB_BUCKET = os.getenv("S3_SOURCE_SUB_BUCKET")

    async with session.client("s3",
                              region_name=AWS_REGION,
                              aws_access_key_id=AWS_ACCESS_KEY,
                              aws_secret_access_key=AWS_SECRET_KEY) as s3:

        for key in keys:
            key = key.strip().rstrip('/')
            updated_key = key.replace("resources/", f"resources/{SOURCE_SUB_BUCKET}/", 1)
            archive_base = f"{archive_prefix}/files"

            # Try to HEAD the object to see if it's a file
            try:
                await s3.head_object(Bucket=SOURCE_BUCKET, Key=updated_key)
                # It's a file — copy it directly
                archive_key = f"{archive_base}/{updated_key}"
                await s3.copy_object(
                    Bucket=archive_bucket,
                    CopySource={'Bucket': SOURCE_BUCKET, 'Key': updated_key},
                    Key=archive_key
                )
                print(f"✅ Copied file: s3://{SOURCE_BUCKET}/{updated_key} ➝ s3://{archive_bucket}/{archive_key}")

            except ClientError as e:
                if e.response['Error']['Code'] in ['404', 'NoSuchKey']:
                    # It's likely a folder — list all contents under it
                    print(f"📁 '{updated_key}' is treated as a folder. Copying all contents...")

                    paginator = s3.get_paginator("list_objects_v2")
                    async for page in paginator.paginate(Bucket=SOURCE_BUCKET, Prefix=updated_key + "/"):
                        contents = page.get("Contents", [])
                        if not contents:
                            print(f"⚠️ No files found in folder s3://{SOURCE_BUCKET}/{updated_key}/")
                            break

                        for obj in contents:
                            obj_key = obj["Key"]
                            dest_key = f"{archive_base}/{obj_key}"
                            try:
                                await s3.copy_object(
                                    Bucket=archive_bucket,
                                    CopySource={'Bucket': SOURCE_BUCKET, 'Key': obj_key},
                                    Key=dest_key
                                )
                                print(f"✅ Copied: s3://{SOURCE_BUCKET}/{obj_key} ➝ s3://{archive_bucket}/{dest_key}")
                            except Exception as ex:
                                print(f"❌ Failed to copy {obj_key}: {ex}")
                else:
                    print(f"❌ Error checking {updated_key}: {e}")

def convert_uuids_to_str(df):
    return df.map(lambda x: str(x) if isinstance(x, asyncpg.pgproto.pgproto.UUID) else x)
