import requests
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv
import os
import logging

# --- Logging Setup ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s — %(levelname)s — %(message)s"
)
log = logging.getLogger(__name__)

load_dotenv()

# --- Snowflake Connection ---
def get_snowflake_connection():
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA")
    )
    log.info("Connected to Snowflake successfully")
    return conn

# --- Fetch CMS Data (v1 API) ---
def fetch_cms_data(session, offset=0, limit=5000):
    url = "https://data.cms.gov/data-api/v1/dataset/14d8e8a9-7e9b-4370-a044-bf97c46b4b44/data"
    params = {
        "size": limit,
        "offset": offset
    }
    response = session.get(url, params=params, timeout=60)
    response.raise_for_status()
    return response.json()

# --- Load Batch into Snowflake ---
def load_to_snowflake(df, conn):
    # Uppercase all column names — Snowflake requirement
    df.columns = [col.upper() for col in df.columns]

    # Replace empty strings with None so Snowflake stores proper NULLs
    df = df.replace("", None)

    success, num_chunks, num_rows, _ = write_pandas(
        conn=conn,
        df=df,
        table_name="PART_D_PRESCRIBERS",
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        auto_create_table=True   # creates the table automatically on first run
    )

    if success:
        log.info(f"Batch loaded successfully — {num_rows} rows written")
    else:
        log.error("Batch load failed")

# --- Main Pipeline ---
def main():
    conn = get_snowflake_connection()
    session = requests.Session()

    offset = 0
    limit = 5000
    total_loaded = 0

    try:
        while True:
            log.info(f"Fetching rows {offset} to {offset + limit}...")
            data = fetch_cms_data(session, offset=offset, limit=limit)

            if not data:
                log.info("No more data returned. Pipeline complete.")
                break

            df = pd.DataFrame(data)
            log.info(f"Fetched {len(df)} rows — loading to Snowflake...")

            load_to_snowflake(df, conn)

            total_loaded += len(df)
            log.info(f"Running total: {total_loaded} rows loaded")

            # Stop if last page (returned fewer rows than requested)
            if len(data) < limit:
                log.info("Last page reached.")
                break

            offset += limit

    except requests.exceptions.RequestException as e:
        log.error(f"API request failed at offset {offset}: {e}")
        raise

    except Exception as e:
        log.error(f"Pipeline failed at offset {offset}: {e}")
        raise

    finally:
        conn.close()
        session.close()
        log.info(f"Pipeline finished. Total rows loaded: {total_loaded}")

if __name__ == "__main__":
    main()