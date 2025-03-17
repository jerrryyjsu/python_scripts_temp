#!/usr/bin/env python3
"""
S3 CSV Handler - A utility for uploading and downloading CSV files to/from AWS S3 buckets.

This script provides functions to:
1. Upload CSV files to an S3 bucket
2. Download CSV files from an S3 bucket
3. List CSV files in an S3 bucket

Requirements:
- boto3
- pandas

Usage:
    python s3_csv_handler.py --upload local_file.csv s3://bucket-name/path/
    python s3_csv_handler.py --download s3://bucket-name/path/file.csv local_directory/
    python s3_csv_handler.py --list s3://bucket-name/path/
"""

import os
import sys
import argparse
import logging
import boto3
import pandas as pd
from botocore.exceptions import ClientError

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

def parse_s3_path(s3_path):
    """Parse an S3 path into bucket name and key."""
    if not s3_path.startswith('s3://'):
        raise ValueError("S3 path must start with 's3://'")
    
    path_without_prefix = s3_path[5:]
    parts = path_without_prefix.split('/', 1)
    
    bucket_name = parts[0]
    key = parts[1] if len(parts) > 1 else ''
    
    return bucket_name, key

def upload_csv_to_s3(local_file_path, s3_path):
    """
    Upload a CSV file to an S3 bucket.
    
    Args:
        local_file_path (str): Path to the local CSV file
        s3_path (str): S3 destination path in format 's3://bucket-name/path/'
    
    Returns:
        bool: True if upload was successful, False otherwise
    """
    if not os.path.exists(local_file_path):
        logger.error(f"Local file not found: {local_file_path}")
        return False
    
    try:
        # Parse S3 path
        bucket_name, key_prefix = parse_s3_path(s3_path)
        
        # Get filename from local path
        filename = os.path.basename(local_file_path)
        
        # Create full S3 key
        if key_prefix and not key_prefix.endswith('/'):
            key_prefix += '/'
        s3_key = f"{key_prefix}{filename}"
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Upload file
        logger.info(f"Uploading {local_file_path} to s3://{bucket_name}/{s3_key}")
        s3_client.upload_file(local_file_path, bucket_name, s3_key)
        
        logger.info(f"Upload successful: s3://{bucket_name}/{s3_key}")
        return True
    
    except ClientError as e:
        logger.error(f"Error uploading file to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def download_csv_from_s3(s3_path, local_directory):
    """
    Download a CSV file from an S3 bucket.
    
    Args:
        s3_path (str): S3 source path in format 's3://bucket-name/path/file.csv'
        local_directory (str): Local directory to save the file
    
    Returns:
        str: Path to the downloaded file if successful, None otherwise
    """
    try:
        # Parse S3 path
        bucket_name, s3_key = parse_s3_path(s3_path)
        
        # Create local directory if it doesn't exist
        if not os.path.exists(local_directory):
            os.makedirs(local_directory)
        
        # Get filename from S3 key
        filename = os.path.basename(s3_key)
        local_file_path = os.path.join(local_directory, filename)
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Download file
        logger.info(f"Downloading s3://{bucket_name}/{s3_key} to {local_file_path}")
        s3_client.download_file(bucket_name, s3_key, local_file_path)
        
        logger.info(f"Download successful: {local_file_path}")
        return local_file_path
    
    except ClientError as e:
        logger.error(f"Error downloading file from S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

def list_csv_files_in_s3(s3_path):
    """
    List all CSV files in an S3 bucket path.
    
    Args:
        s3_path (str): S3 path in format 's3://bucket-name/path/'
    
    Returns:
        list: List of CSV files in the specified path
    """
    try:
        # Parse S3 path
        bucket_name, prefix = parse_s3_path(s3_path)
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # List objects
        logger.info(f"Listing CSV files in s3://{bucket_name}/{prefix}")
        response = s3_client.list_objects_v2(
            Bucket=bucket_name,
            Prefix=prefix
        )
        
        # Filter for CSV files
        csv_files = []
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                if key.lower().endswith('.csv'):
                    csv_files.append(f"s3://{bucket_name}/{key}")
        
        return csv_files
    
    except ClientError as e:
        logger.error(f"Error listing files in S3: {e}")
        return []
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return []

def read_csv_from_s3(s3_path):
    """
    Read a CSV file directly from S3 into a pandas DataFrame.
    
    Args:
        s3_path (str): S3 path in format 's3://bucket-name/path/file.csv'
    
    Returns:
        DataFrame: Pandas DataFrame containing the CSV data
    """
    try:
        # Parse S3 path
        bucket_name, key = parse_s3_path(s3_path)
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Get object
        logger.info(f"Reading CSV from s3://{bucket_name}/{key}")
        obj = s3_client.get_object(Bucket=bucket_name, Key=key)
        
        # Read CSV into DataFrame
        df = pd.read_csv(obj['Body'])
        
        logger.info(f"Successfully read CSV with {len(df)} rows")
        return df
    
    except ClientError as e:
        logger.error(f"Error reading CSV from S3: {e}")
        return None
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return None

def write_dataframe_to_s3(df, s3_path):
    """
    Write a pandas DataFrame directly to a CSV file in S3.
    
    Args:
        df (DataFrame): Pandas DataFrame to write
        s3_path (str): S3 destination path in format 's3://bucket-name/path/file.csv'
    
    Returns:
        bool: True if write was successful, False otherwise
    """
    try:
        # Parse S3 path
        bucket_name, key = parse_s3_path(s3_path)
        
        # Create a buffer
        csv_buffer = df.to_csv(index=False)
        
        # Create S3 client
        s3_client = boto3.client('s3')
        
        # Upload to S3
        logger.info(f"Writing DataFrame with {len(df)} rows to s3://{bucket_name}/{key}")
        s3_client.put_object(
            Bucket=bucket_name,
            Key=key,
            Body=csv_buffer
        )
        
        logger.info(f"Successfully wrote DataFrame to s3://{bucket_name}/{key}")
        return True
    
    except ClientError as e:
        logger.error(f"Error writing DataFrame to S3: {e}")
        return False
    except Exception as e:
        logger.error(f"Unexpected error: {e}")
        return False

def main():
    """Main function to handle command line arguments."""
    parser = argparse.ArgumentParser(description='S3 CSV Handler')
    
    # Create a mutually exclusive group for commands
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('--upload', action='store_true', help='Upload a CSV file to S3')
    group.add_argument('--download', action='store_true', help='Download a CSV file from S3')
    group.add_argument('--list', action='store_true', help='List CSV files in an S3 bucket path')
    
    # Add positional arguments
    parser.add_argument('source', help='Source path (local file or S3 path)')
    parser.add_argument('destination', help='Destination path (S3 path or local directory)')
    
    args = parser.parse_args()
    
    if args.upload:
        success = upload_csv_to_s3(args.source, args.destination)
        sys.exit(0 if success else 1)
    
    elif args.download:
        local_file = download_csv_from_s3(args.source, args.destination)
        sys.exit(0 if local_file else 1)
    
    elif args.list:
        csv_files = list_csv_files_in_s3(args.source)
        for file in csv_files:
            print(file)
        sys.exit(0)

if __name__ == "__main__":
    main()