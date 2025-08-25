import os
import asyncpg
from dotenv import load_dotenv

load_dotenv()

async def get_connection():
    return await asyncpg.connect(
        user=os.getenv('DB_USER'),
        password=os.getenv('DB_PASSWORD'),
        database=os.getenv('DB_NAME'),
        host=os.getenv('DB_HOST'),
        port=os.getenv('DB_PORT')
    )


async def fetch_data(sql_path, *params):
    conn = await get_connection()
    try:
        with open(sql_path, 'r') as file:
            query = file.read()

        interpolated = interpolate_query(query, params)
        print("📥 Executed SQL:")
        print(interpolated)

        stmt = await conn.prepare(query)
        records = await stmt.fetch(*params)  # Unpack positional parameters
        if records:
            columns = records[0].keys()
            rows = [tuple(row.values()) for row in records]
            return rows, columns
        return [], []
    finally:
        await conn.close()

def interpolate_query(query: str, params: tuple) -> str:
    for i, param in enumerate(params, 1):
        if isinstance(param, str):
            value = f"'{param}'"
        elif param is None:
            value = "NULL"
        else:
            value = str(param)
        query = query.replace(f"${i}", value)
    return query


