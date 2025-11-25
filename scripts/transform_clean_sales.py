import pandas as pd
from utils.logger import setup_logger

logger = setup_logger(__name__)

def clean_and_transform_sales_data(input_file, output_file):
    """
    Clean and transform raw sales data
    """
    logger.info(f"Transforming data from {input_file}")
    
    df = pd.read_csv(input_file)
    
    # Data cleaning steps
    df.dropna(subset=['order_id', 'customer_id'], inplace=True)
    df['order_date'] = pd.to_datetime(df['order_date'])
    df['total'] = df['quantity'] * df['price']
    df.columns = [col.strip().lower().replace(' ', '_') for col in df.columns]
    
    # Save cleaned data
    df.to_csv(output_file, index=False)
    
    logger.info(f"Cleaned data saved to {output_file}")
    return output_file
