import os
from datetime import datetime, timedelta
from database import fetch_data
from s3_utils import ensure_bucket_exists, set_lifecycle_policy, upload_parquet_from_path, convert_uuids_to_str
from dotenv import load_dotenv
import pandas as pd

load_dotenv()

def main():
    cutoff_date = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d')
    bucket = os.getenv("S3_BUCKET")

    ensure_bucket_exists(bucket)

    queries_dir = "queries"
    sql_files = [f for f in os.listdir(queries_dir) if f.endswith(".sql")]

    today_folder = datetime.now().strftime("%b%d%Y").lower()
    temp_dir = f"temp_parquet/{today_folder}"
    os.makedirs(temp_dir, exist_ok=True)

    files_to_upload = []

    try:
        for filename in sql_files:
            filepath = os.path.join(queries_dir, filename)

            # Skip empty or whitespace-only SQL files
            with open(filepath, 'r') as f:
                content = f.read().strip()
            if not content:
                print(f"⚠️ Skipping empty SQL file: {filename}")
                continue

            prefix = os.path.splitext(filename)[0]
            print(f"📥 Fetching data for: {prefix}")

            data, columns = fetch_data(filepath, {"cutoff_date": cutoff_date})

            if not data:
                print(f"ℹ️ No data found in: {prefix}. Skipping.")
                continue

            df = pd.DataFrame(data, columns=columns)
            df = convert_uuids_to_str(df)  # convert UUID columns to strings

            local_path = os.path.join(temp_dir, f"{prefix}.parquet")
            df.to_parquet(local_path, index=False)

            files_to_upload.append(local_path)

        if not files_to_upload:
            print("ℹ️ No data to upload from any query. Exiting.")
            return

        # Upload all parquet files now
        for local_path in files_to_upload:
            upload_parquet_from_path(local_path, bucket)

        print("✅ All files uploaded successfully.")

    except Exception as e:
        print(f"❌ Error occurred: {e}")
        print("⏹️ Aborting without uploading any files.")

        # Clean up local parquet files
        for fpath in files_to_upload:
            if os.path.exists(fpath):
                os.remove(fpath)
        exit(1)

if __name__ == "__main__":
    main()
