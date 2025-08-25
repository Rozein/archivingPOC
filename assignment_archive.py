import os
from datetime import datetime, timedelta
from database import fetch_data
import pandas as pd

from s3_utils import upload_parquet_from_dataframe, copy_s3_files_from_keys


def get_today_folder():
    return datetime.now().strftime("%b%d%Y").lower()

async def archive_assignments(cutoff_date, bucket, queries_dir="queries"):
    sql_files = [f for f in os.listdir(queries_dir) if f.endswith(".sql") and f.startswith("assignment")]
    assignment_sql = next((f for f in sql_files if f == "assignment.sql"), None)

    path = os.path.join(queries_dir, assignment_sql)

    if assignment_sql:
        sql_files.remove(assignment_sql)

    print("📥 Fetching assignment data...")
    data, columns = await fetch_data(path,  cutoff_date)

    if not data:
        print("ℹ️ No data in assignment.sql. Skipping all archiving.")
        return  # Exit early, no files uploaded

    df = pd.DataFrame(data, columns=columns)
    assignment_ids = df["Id"].tolist()
    dependent_dfs = []
    s3_keys_to_copy = []


    for filename in sql_files:
        path = os.path.join(queries_dir, filename)
        if os.stat(path).st_size == 0 or not open(path).read().strip():
            print(f"⚠️ Skipping empty file: {filename}")
            continue

        dep_data, dep_columns = await fetch_data(path, assignment_ids)
        if not dep_data:
            print(f"ℹ️ No data found for {filename}.")
            continue

        df = pd.DataFrame(dep_data, columns=dep_columns)
        dependent_dfs.append((filename, df))

        if filename == "assignment_attachment.sql" and "AttachmentUrl" in df.columns and "AttachmentType" in df.columns:
            filtered_df = df[
                (df["AttachmentType"] == 2) &
                (df["AttachmentUrl"].notnull()) &
                (df["AttachmentUrl"].str.strip() != "")
                ]
            cleaned_keys = filtered_df["AttachmentUrl"].apply(lambda x: x.rstrip('/')).tolist()
            s3_keys_to_copy.extend(cleaned_keys)

    print("✅ All data fetched. Uploading to S3...")
    today_folder = get_today_folder()

    try:
        await upload_parquet_from_dataframe(df, bucket, f"{today_folder}/assignment.parquet")

        for filename, df in dependent_dfs:
            prefix_name = os.path.splitext(filename)[0]
            await upload_parquet_from_dataframe(df, bucket, f"{today_folder}/{prefix_name}.parquet")
            print("✅ Parquet files uploaded.")

        if s3_keys_to_copy:
            await copy_s3_files_from_keys(s3_keys_to_copy, bucket, today_folder)
            print("✅ Completed S3 Copy.")


    except Exception as e:
        print(f"❌ Upload failed: {e}")
        print("⚠️ Partial upload prevented. Nothing was finalized.")

        await upload_parquet_from_dataframe(df, bucket, f"{today_folder}/{prefix_name}.parquet")




