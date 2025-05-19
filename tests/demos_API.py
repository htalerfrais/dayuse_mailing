import os
import snowflake.connector
from dotenv import load_dotenv
import json
import pandas as pd
from typing import Dict, List, Optional, Tuple, Union
from snowflake.demos import help, load_demo, teardown

load_dotenv()


def get_snowflake_connection():  
    conn = snowflake.connector.connect(
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        host=os.getenv("SNOWFLAKE_HOST"),
        role=os.getenv("SNOWFLAKE_ROLE"),
        )
    return conn


conn = get_snowflake_connection()
cur = conn.cursor()

print(cur.execute("select 1"))

print(help())

demo = load_demo('analytics-cortex')

print(demo)
