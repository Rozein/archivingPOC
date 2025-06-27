import os
import asyncio
from datetime import datetime, timedelta
from dotenv import load_dotenv
from s3_utils import ensure_bucket_exists
from assignment_archive import archive_assignments

load_dotenv()

async def main():
    cutoff_date = datetime.now() - timedelta(days=90)
    bucket = os.getenv("S3_BUCKET")

    await ensure_bucket_exists(bucket)
    await archive_assignments(cutoff_date, bucket)

if __name__ == "__main__":
    asyncio.run(main())
