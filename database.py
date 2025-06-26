import os
from urllib.parse import quote_plus

from sqlalchemy import create_engine, text
from dotenv import load_dotenv


load_dotenv()

def get_engine():
    user = os.getenv('DB_USER')
    password = quote_plus(os.getenv('DB_PASSWORD'))  # Encode special characters
    host = os.getenv('DB_HOST')
    port = os.getenv('DB_PORT')
    dbname = os.getenv('DB_NAME')

    db_url = f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{dbname}"
    return create_engine(db_url)

def fetch_data(sql_file_path, params):
    engine = get_engine()
    with open(sql_file_path, 'r') as file:
        query = text(file.read())

    with engine.connect() as conn:
        result = conn.execute(query, params)
        data = result.fetchall()
        columns = result.keys()
        return data, columns