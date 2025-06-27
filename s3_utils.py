import os
import aioboto3
from io import BytesIO
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
                        'Days': 30,
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

def convert_uuids_to_str(df):
    return df.map(lambda x: str(x) if isinstance(x, asyncpg.pgproto.pgproto.UUID) else x)
