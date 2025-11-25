import snowflake.connector
import yaml

def get_snowflake_connection():
    """
    Create and return Snowflake connection
    """
    with open('config/snowflake_config.yaml', 'r') as f:
        config = yaml.safe_load(f)
    
    conn = snowflake.connector.connect(
        user=config['user'],
        password=config['password'],
        account=config['account'],
        warehouse=config['warehouse'],
        database=config['database'],
        schema=config['schema']
    )
    
    return conn
