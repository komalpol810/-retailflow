import os
import pandas as pd
from sqlalchemy import create_engine, text
from datetime import datetime, timezone

DATA_DIR = "data"
DB_URI = "postgresql://deuser:depass@localhost:5432/raw_db"
SCHEMA = "raw"

def get_engine():
    return create_engine(DB_URI)

def ensure_schema(engine):
    with engine.begin() as conn:
        conn.execute(text(f"CREATE SCHEMA IF NOT EXISTS {SCHEMA}"))

def load_csv_as_text(engine, filepath):
    filename = os.path.basename(filepath)
    table_name = os.path.splitext(filename)[0].lower()

    df = pd.read_csv(filepath, dtype=str)
    df["_ingested_at"] = datetime.now(timezone.utc)
    df["_source_file"] = filename

    df.to_sql(
        table_name,
        engine,
        schema=SCHEMA,
        if_exists="replace",
        index=False,
        method="multi",
        chunksize=5000
    )
    print(f"Loaded {len(df)} rows into {SCHEMA}.{table_name}")

def main():
    engine = get_engine()
    ensure_schema(engine)
    for fname in os.listdir(DATA_DIR):
        if fname.endswith(".csv"):
            load_csv_as_text(engine, os.path.join(DATA_DIR, fname))

if __name__ == "__main__":
    main()
