from datetime import datetime
import logging
import subprocess
import tempfile
import requests
import boto3
import json
import botocore.exceptions
import os
from utils.config_manager import load_configs, get_destination_output_path_template, get_country_config
from utils.report_processor import fetch_report_data
from utils.common_utils import format_date, get_env_var

"""
Walmart API Report Processing Utility

Created Date: 2024-08-13
Version: 1.0.0
Created By: Anamika
Purpose: This script provides functions to create, fetch, process, and upload Walmart API reports to S3.

Functionality:
    - `WalmartReportProcessor.__init__`: Initializes the report processor with country code, S3 bucket name, configuration data, and environment settings.
    - `WalmartReportProcessor.generate_auth_signature`: Generates an authentication signature using a Java JAR file.
    - `WalmartReportProcessor.fetch_report_urls_via_get`: Fetches the report URLs from the Walmart API using a GET request.
    - `WalmartReportProcessor.fetch_report_urls_via_post`: Fetches the report URLs from the Walmart API using a POST request with a specified report date.
    - `WalmartReportProcessor.process_report`: Processes the report configuration and fetches the corresponding report URLs for downloading and uploading to S3.

Dependencies:
    - requests: For HTTP requests to interact with the Walmart API.
    - boto3: For AWS S3 interactions.
    - json: For handling JSON data.
    - logging: For logging operations and errors.
    - subprocess: For executing the Java JAR for signature generation.
    - tempfile: For creating temporary files for the Java JAR and private key.
    - os: For file system operations.
    - botocore.exceptions: For handling AWS-specific exceptions.
    - utils.config_manager: For loading and managing configuration data.
    - utils.report_processor: For processing and uploading the reports to S3.
    - utils.common_utils: For utility functions like date formatting.

"""


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class WalmartReportProcessor:
    def __init__(self, country_code, config,consumer_id, consumer_version, config_path, jar_path, bucket_nm):
        """
        Initialize the WalmartReportProcessor class with configuration details.

        Args:
            country_code (str): The country code for the Walmart API.
            s3_bucket_name (str): The name of the S3 bucket.
            config_data (dict): The configuration dictionary.
            environment (str): The environment setting (e.g., 'prod', 'dev').
        """
        self.country_code = country_code
        self.config_data = config
        self.bucket_nm = bucket_nm
        self.consumer_id = consumer_id
        self.consumer_version = consumer_version
        self.config_path = config_path
        self.jar_path = jar_path

    def generate_auth_signature(self):
        """
        Generate authentication signature using a Java JAR file.

        Args:
            auth_headers (dict): The headers required for authentication.

        Returns:
            tuple: A tuple containing the signature and timestamp.
        """
        logger.info("Generating authentication signature")
        try:
            private_key = get_env_var("MWAA_WALMART_PRIVATE_KEY")
            logger.info(f"private_key:{private_key}")
            private_key_file_path = None
            jar_file_path = self.jar_path
            logger.info(f"Using JAR file: {jar_file_path}")

            # Create a temporary private key file
            with tempfile.NamedTemporaryFile(delete=False) as temp_key:
                temp_key.write(private_key.encode('utf-8'))  # Write the private key to the temp file
                private_key_file_path = temp_key.name

            # Execute the Java JAR file for signature generation
            subprocess_execution = subprocess.Popen(
                ['java', '-jar', jar_file_path, self.consumer_id, private_key_file_path, self.consumer_version],
                stdout=subprocess.PIPE, stderr=subprocess.PIPE
            )
            stdout_output, stderr_output = subprocess_execution.communicate()

            if subprocess_execution.returncode != 0:
                logger.error(f"Error running Java command. Output: {stdout_output.decode('utf-8')}, Error: {stderr_output.decode('utf-8')}")
                raise subprocess.CalledProcessError(
                    subprocess_execution.returncode, 
                    ' '.join(subprocess_execution.args), 
                    output=stdout_output.decode('utf-8'), 
                    stderr=stderr_output.decode('utf-8')
                )

            command_output = stdout_output.decode('utf-8').strip()
            output_lines = command_output.split('\n')

            if len(output_lines) >= 2:
                auth_signature = output_lines[0].split(': \t')[1].strip()
                auth_timestamp = output_lines[1].split(': \t')[1].strip()
                logger.info(f"Signature: {auth_signature}, Timestamp: {auth_timestamp}")
                return auth_signature, auth_timestamp
            else:
                raise ValueError("Unexpected output format from Java process")
                

        finally:
            if private_key_file_path and os.path.exists(private_key_file_path):
                os.remove(private_key_file_path)  # Ensure the temporary key file is deleted


    def fetch_report_urls_via_get(self, report_type_name, request_headers, api_url):
        """
        Fetch data from Walmart API and return download URLs.

        Args:
            report_type_name (str): The type of the report to fetch.
            request_headers (dict): The headers for the API request.
            report_config_data (dict): The configuration for the report.

        Returns:
            list: A list of URLs for the downloadable report files.
        """
        try: 
            response = requests.get(api_url, headers=request_headers)
            response.raise_for_status()
            response_json_data = response.json()
            logger.info(f"Data fetched successfully for {report_type_name}")

            downloadable_report_urls = response_json_data.get('downloadUrls', [])
            if not downloadable_report_urls:
                logger.warning(f"No files found for {report_type_name}")
                return []

            report_file_urls = [file_info['url'] for file_info in downloadable_report_urls]
            return report_file_urls

        except requests.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            return []
        except ValueError as e:
            logger.error(f"Error parsing JSON response: {e}")
            return []
        except Exception as e:
            logger.error(f"Error in fetching data for {report_type_name}: {e}")
            return []
        
    def fetch_report_urls_via_post(self, report_type_name, request_headers, api_url, report_date):
        """
        Sends a POST request to Walmart API to fetch report data based on a specified date.

        Args:
            report_type_name (str): The type of the report to fetch.
            request_headers (dict): The headers required for authentication and API request.
            report_config_data (dict): The configuration for the report (including the URL).
            report_date (str): The date for which the report is to be fetched.

        Returns:
            list: A list of URLs for the downloadable report files, or an empty list if an error occurs.
        """
        logging.info(f"config_url: {api_url}")
        payload = json.dumps({
            "feedDate": report_date  # The payload includes the feed date.
        })

        try:
            # Send the POST request to the Walmart API
            response = requests.post(api_url, headers=request_headers, data=payload)
            logging.info(f"response:{response}")

            # Raise an exception if the request resulted in an HTTP error
            response.raise_for_status()

            # Parse the JSON response
            response_json_data = response.json()

            # Extract the download URLs from the response
            downloadable_report_urls = response_json_data.get('downloadUrls', [])
            if not downloadable_report_urls:
                logger.warning(f"No files found for {report_type_name}")
                return []
            report_file_urls = [file_info['url'] for file_info in downloadable_report_urls]
            return report_file_urls
        
        except requests.HTTPError as e:
            logger.error(f"HTTP error occurred: {e}")
            raise 
        except ValueError as e:
            logger.error(f"Error parsing JSON response: {e}")
            raise
        except Exception as e:
            logger.error(f"Error in fetching data for {report_type_name}: {e}")
            raise


    def process_report(self, report_type_name, report_date):
        """
        Processes the configuration for a specific report type.

        Args:
            report_type_name (str): The type of the report to process.
        """
        try:
            # Fetch country-specific configuration
            self.destination_path_template = get_destination_output_path_template(self.config_data)
            self.content_type = self.config_data.get('content_type', '')
            reports_config = self.config_data.get('report_configs', {})
            report_config_key = next(((k, v) for k, v in reports_config.items() if k.lower() == report_type_name), (None, None))
            # Unpack the results
            key, value = report_config_key
            logger.info(f"s3_report_config_key:{report_config_key}")
            if not report_config_key:
                logger.error(f"No configuration found for report type: {report_type_name}")
                return

            try:
                # Load the specific report config from S3
                key_config_path = f"{'/'.join(self.config_path.split('/')[:-1])}/{value}"
                logging.info(f"key_config_path:{key_config_path}")
                value_config = load_configs(key_config_path)
                report_config = get_country_config(value_config, self.country_code)
            except Exception as e:
                logger.error(f"Failed to load configuration from S3 for report {report_type_name}: {e}")
                return
            report_config_data = report_config
            logging.info(f"report_config_data : {report_config_data}")
            try:
                if not report_date:
                    # Generate signature for authentication
                    auth_signature, auth_timestamp = self.generate_auth_signature()
                    auth_request_headers = {
                        'WM_CONSUMER.ID': self.consumer_id,
                        'WM_SEC.AUTH_SIGNATURE': auth_signature,
                        'WM_CONSUMER.INTIMESTAMP': auth_timestamp,
                        'WM_CONSUMER.VERSION': self.consumer_version
                    }
                    logging.info(f"auth_request_headers:{auth_request_headers}")
                    logging.info(f"Fetching data from Walmart API for {report_type_name}")
                    api_url = report_config_data.get('url', '')
                    logging.info(f"api_url:{api_url}")
                    download_report_urls = self.fetch_report_urls_via_get(key, auth_request_headers, api_url)
                    report_date = datetime.now()
                else:
                    report_date = format_date(report_date)
                    api_url = report_config_data.get('url', '')
                    auth_signature, auth_timestamp = self.generate_auth_signature()
                    auth_request_headers = {
                        'WM_CONSUMER.ID': self.consumer_id,
                        'WM_SEC.AUTH_SIGNATURE': auth_signature,
                        'WM_CONSUMER.INTIMESTAMP': auth_timestamp,
                        'WM_CONSUMER.VERSION': self.consumer_version,
                        'Content-Type': self.content_type
                    }
                    
                    logging.info(f"auth_request_headers:{auth_request_headers}")
                    logging.info(f"Fetching data from Walmart API for {report_type_name}")
                    download_report_urls = self.fetch_report_urls_via_post(key, auth_request_headers, api_url, report_date)
                    report_date = datetime.strptime(report_date, '%Y-%m-%d')
                if download_report_urls:
                    logger.info(f"Report URLs: {download_report_urls}")

                    # Process each URL
                    for url in download_report_urls:
                        fetch_report_data(url, key, self.bucket_nm, self.destination_path_template, self.country_code, report_date)

            except Exception as e:
                logger.error(f"Error processing report {key}: {e}")
                return

        except Exception as e:
            logger.error(f"Error in process_report method: {e}")
            return