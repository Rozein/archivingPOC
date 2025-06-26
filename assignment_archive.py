import os
from datetime import datetime, timedelta
from database import fetch_data
import pandas as pd


def archive_assignments(cutoff_date, bucket, queries_dir="queries"):
    sql_files = [f for f in os.listdir(queries_dir) if f.endswith(".sql") and f.startswith("assignment")]
    assignment_sql = next((f for f in sql_files if f == "assignment.sql"), None)

    if assignment_sql:
        sql_files.remove(assignment_sql)

    print("📥 Fetching assignment data...")
    data, columns = fetch_data(assignment_sql, {"cutoff_date": cutoff_date})

    if not data:
        print("ℹ️ No data in assignment.sql. Skipping all archiving.")
        return  # Exit early, no files uploaded

    df = pd.DataFrame(data, columns=columns)
    assignment_ids = df["id"].tolist()



