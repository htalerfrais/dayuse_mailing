import snowflake.connector
import os
from dotenv import load_dotenv
load_dotenv()


def get_snowflake_connection():
    # Ensure you replace these paths with your actual paths
    diag_log_path = "~/SnowflakeConnectionTestReport.txt" # Changed to relative path
    allowlist_file_path = os.path.expanduser("~/snowflake_allowlist.json") # Path to your saved allowlist JSON    

    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        host=os.getenv("SNOWFLAKE_HOST"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        # to debug connection issues
        enable_connection_diag=True,
        connection_diag_log_path=diag_log_path,
        connection_diag_allowlist_path=allowlist_file_path
        )
    return conn


conn = get_snowflake_connection()
cur = conn.cursor()
cur.execute("")
