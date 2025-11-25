import boto3
import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)

def extract_sales_data_from_s3(bucket_name, prefix, local_path):
    """
    Extract sales data from AWS S3
    """
    logger.info(f"Extracting data from S3 bucket: {bucket_name}/{prefix}")
    
    s3_client = boto3.client('s3')
    
    # List and download files
    response = s3_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    
    all_data = []
    for obj in response.get('Contents', []):
        if obj['Key'].endswith('.csv'):
            logger.info(f"Downloading {obj['Key']}")
            obj_data = s3_client.get_object(Bucket=bucket_name, Key=obj['Key'])
            df = pd.read_csv(obj_data['Body'])
            all_data.append(df)
    
    # Combine all CSV files
    combined_df = pd.concat(all_data, ignore_index=True)
    combined_df.to_csv(local_path, index=False)
    
    logger.info(f"Data extracted and saved to {local_path}")
    return local_path
