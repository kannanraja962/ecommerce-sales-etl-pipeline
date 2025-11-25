import snowflake.connector
import pandas as pd
from utils.logger import setup_logger
from utils.snowflake_connector import get_snowflake_connection

logger = setup_logger(__name__)

def load_data_to_snowflake(csv_file, table_name, database, schema):
    """
    Load cleaned data into Snowflake
    """
    logger.info(f"Loading data to Snowflake table: {database}.{schema}.{table_name}")
    
    conn = get_snowflake_connection()
    cursor = conn.cursor()
    
    try:
        # Use Snowflake's PUT and COPY INTO commands
        cursor.execute(f"USE DATABASE {database}")
        cursor.execute(f"USE SCHEMA {schema}")
        
        # Stage the file
        cursor.execute(f"PUT file://{csv_file} @%{table_name}")
        
        # Copy into table
        cursor.execute(f"""
            COPY INTO {table_name}
            FROM @%{table_name}
            FILE_FORMAT = (TYPE = 'CSV' FIELD_OPTIONALLY_ENCLOSED_BY='"' SKIP_HEADER=1)
            ON_ERROR = 'CONTINUE'
        """)
        
        logger.info(f"Data successfully loaded into {table_name}")
        
    except Exception as e:
        logger.error(f"Error loading data: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()
