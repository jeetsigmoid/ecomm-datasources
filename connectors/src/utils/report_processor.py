import os
import logging
import requests
import gzip
import json
import pandas as pd
import pyarrow.orc as orc
import boto3
from io import BytesIO, StringIO
import tempfile
import shutil
from datetime import datetime
from urllib.parse import urlparse

"""
Report Data Processing Utility

Created Date: 2024-08-13
Version: 1.0.0
Created By: Anamika
Purpose: This script provides functions to download, process, and upload Amazon Advertising API reports. It handles gzip file formats, converts data to CSV, and uploads the results to an S3 bucket.

Functionality:
    - `fetch_report_data`: Downloads, processes, and uploads report data.
    - `decompress_file`: Decompresses gzip files.
    - `load_json`: Loads JSON data from a file.
    - `convert_json_to_csv`: Converts JSON data to CSV format.
    - `convert_orc_to_csv`: Converts ORC data to CSV format.
    - `upload_to_destination`: Uploads a CSV file to S3.
    - `cleanup_files`: Removes temporary files.

Dependencies:
    - requests: For HTTP requests to download report data.
    - gzip: For decompressing gzip files.
    - json: For handling JSON data.
    - pandas: For converting JSON to CSV.
    - pyarrow: For handling ORC file formats.
    - boto3: For AWS S3 interactions.
    - tempfile: For creating temporary files.
    - shutil: For file operations.
    - logging: For logging operations and errors.
"""

def decompress_file(file_path):
    """
    Decompress a gzip file and return the path to the decompressed file.

    Args:
        file_path (str): Path to the gzip file to decompress.

    Returns:
        str: Path to the decompressed file.
    """
    decompressed_path = file_path.replace('.gz', '')
    with gzip.open(file_path, 'rb') as f_in:
        with open(decompressed_path, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)
    logging.info(f"decompressed_path: {decompressed_path}")
    return decompressed_path

def load_json(file_path):
    """
    Load JSON data from a file.

    Args:
        file_path (str): Path to the JSON file.

    Returns:
        dict: JSON data.
    """
    with open(file_path, 'r') as f:
        return json.load(f)

def convert_json_to_csv(json_data):
    """
    Convert JSON data to a CSV file.

    Args:
        json_data (dict): JSON data to convert.

    Returns:
        str: Path to the generated CSV file.
    """
    csv_filename = '/tmp/report.csv'
    df = pd.json_normalize(json_data)
    df.to_csv(csv_filename, index=False)
    logging.info(f"csv_filename: {csv_filename}")
    return csv_filename

def convert_orc_to_csv(file_path):
    """
    Convert ORC file to CSV file.

    Args:
        file_path (str): Path to the ORC file.

    Returns:
        str: Path to the generated CSV file.
    """
    with open(file_path, 'rb') as f:
        buf = BytesIO(f.read())
    table = orc.read_table(buf)
    df = table.to_pandas()
    csv_filename = file_path.replace('.orc', '.csv')
    df.to_csv(csv_filename, index=False)
    return csv_filename

def upload_to_destination(file_path, report_type, destination_bucket_name, destination_path_template, country_code, date):
    """
    Upload a file to S3.

    Args:
        file_path (str): Path to the file to upload.
        report_type (str): Type of the report.
        s3_bucket_name (str): The name of the S3 bucket.
        destination_path_template (str): The S3 path template.
        country_code (str): Country code for the report.
    """
    s3 = boto3.client('s3')
    year = date.strftime("%Y")
    month = date.strftime("%m")
    day = date.strftime("%d")
    bucket_name = destination_bucket_name
    s3_key = destination_path_template.format(
        report_type=report_type,
        country_code = country_code,
        year=year,
        month=month,
        day=day
    )

    s3.upload_file(file_path, bucket_name, s3_key)
    logging.info(f"Uploaded file {file_path} to S3 bucket {bucket_name} with key {s3_key}")

def cleanup_files(file_paths):
    """
    Remove temporary files.

    Args:
        file_paths (list): List of file paths to remove.
    """
    for file_path in file_paths:
        if os.path.exists(file_path):
            os.remove(file_path)
            logging.info(f"Removed temporary file: {file_path}")

def fetch_report_data(report_url, report_type, s3_bucket_name, destination_path_template, country_code,date):
    """
    Downloads, decompresses, and processes the report data. Saves it to a CSV file and uploads to S3.

    Args:
        report_url (str): URL from which to download the report data.
        report_type (str): Type of the report for which the data is processed.
        s3_bucket_name (str): The name of the S3 bucket where the files will be uploaded.
        destination_path_template (str): The S3 path template for the uploaded files.
        country_code (str): Country code for the report.

    Raises:
        Exception: For errors encountered during data processing and S3 upload.
    """
    try:

        parsed_url = urlparse(report_url)
        path = parsed_url.path.lower()
          # Step 1: Inspect the URL for keywords to determine the file extension
        if path.endswith('.orc'):
            file_extension = '.orc'
        elif path.endswith('.gz'):
            file_extension = '.gz'
        elif path.endswith('.csv'):
            file_extension = '.csv'
        else:
            raise ValueError("Could not determine file extension from URL")

        # Step 2: Download the report data
        response = requests.get(report_url)
        response.raise_for_status()

        # Step 3: Save the downloaded content to a temporary file with the correct extension
        with tempfile.NamedTemporaryFile(delete=False, suffix=file_extension) as temp_file:
            local_filename_with_extension = temp_file.name
            logging.info(f"Saving file as: {local_filename_with_extension}")
            with open(local_filename_with_extension, 'wb') as f:
                f.write(response.content)


        # Check file type and process accordingly
        if file_extension == '.gz':
            decompressed_filename = decompress_file(local_filename_with_extension)
            report_data = load_json(decompressed_filename)
            csv_filename = convert_json_to_csv(report_data)
        elif file_extension == '.orc':
            csv_filename = convert_orc_to_csv(local_filename_with_extension)
        elif file_extension == '.csv':
            csv_filename = local_filename_with_extension
        else:
            raise ValueError("Unsupported file format")

        # Upload the CSV file to S3
        upload_to_destination(csv_filename, report_type, s3_bucket_name, destination_path_template, country_code,date)

        # Step 6: Clean up temporary files
        cleanup_files([local_filename_with_extension, decompressed_filename, csv_filename] if file_extension == '.gz' else [local_filename_with_extension, csv_filename])

    except Exception as e:
        logging.error(f"Error fetching report data: {e}")
        raise


def s3_manual_upload(destination_bucket, destination_path, df, max_date, report_type, country_code):
    """
    Uploads a file to an Amazon S3 bucket with a dynamically generated S3 key based on the current date.

    Parameters:
    destination_bucket (str): The name of the S3 bucket where the file will be uploaded.
    destination_path (str): The S3 path where the file will be stored, with placeholders for dynamic components
                            such as report type, year, month, and day.

    Raises:
    Exception: If any error occurs during the upload process, the exception is logged and re-raised.
    """
    try:
        # de/product_map_amazon/US/destination_file
        # destination_path = 'de/product_map_amazon/US/destination_file/{report_type}_{year}_{month}_{day}.csv'
        s3 = boto3.client('s3')

        # Get current date info
        year = max_date.strftime("%Y")
        month = max_date.strftime("%m")
        day = max_date.strftime("%d")
        
        # Format S3 key
        s3_key = destination_path.format(
            country_code=country_code,
            report_type=report_type,
            year=year,
            month=month,
            day=day
        )

        
        # Convert DataFrame to CSV and store in buffer
        csv_buffer = StringIO()
        df.to_csv(csv_buffer, index=False)

        # Upload the CSV to S3
        s3.put_object(Bucket=destination_bucket, Key=s3_key, Body=csv_buffer.getvalue())
        logging.info(f"Uploaded DataFrame to S3 bucket {destination_bucket} with key {s3_key}")

    except Exception as e:
        logging.error(f"An error occurred in upload_to_s3: {e}")