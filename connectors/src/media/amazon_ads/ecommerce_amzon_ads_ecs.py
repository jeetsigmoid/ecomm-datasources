from datetime import datetime
import time
import json
import requests
import logging
from utils.config_manager import load_configs, get_api_urls, get_destination_output_path_template,get_country_config
from utils.common_utils import generate_access_token, fetch_profile_id, format_date
from utils.report_processor import fetch_report_data  

"""
Amazon Advertising API Report Processing Utility

Created Date: 2024-08-13
Version: 1.0.0
Created By: Anamika
Purpose: This script provides functions to create, fetch, process, and upload Amazon Advertising API reports.vertisin

Functionality:
    - `AmazonAdvertisingAPI.__init__`: Initializes the API client with country code, bucket name, and config.
    - `AmazonAdvertisingAPI.get_country_config`: Retrieves the configuration for the specified country.
    - `AmazonAdvertisingAPI.create_report_request`: Creates a report request using the Amazon Advertising API.
    - `AmazonAdvertisingAPI.fetch_report_url`: Fetches the URL(s) of the generated report.
    - `AmazonAdvertisingAPI.process_report_config`: Processes the configuration for generating and handling reports.

Dependencies:
    - requests: For HTTP requests to interact with the Amazon Advertising API.
    - gzip: For decompressing gzip files.
    - json: For handling JSON data.
    - csv: For writing CSV files.
    - logging: For logging operations and errors.
    - boto3: For AWS S3 interactions.
    - datetime: For working with dates and times.
    - time: For sleep and retry logic.
    - os: For file system operations.
"""


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class AmazonAdvertisingReportProcessor:
    def __init__(self, country_code,config,bucket_nm,config_path, client_id, client_secret, refresh_token):
        """
        Initializes the AmazonAdvertisingAPI class with the given parameters.

        Args:
            country_code (str): Country code for configuration.
            bucket_nm (str): S3 bucket name where configurations are stored.
            config (dict): Global configuration dictionary.

        Raises:
            Exception: For errors encountered during initialization.
        """
        try:
            # Load the global configuration
            self.country_code = country_code
            self.config = config
            self.bucket_nm = bucket_nm
            self.config_path = config_path
            self.client_id = client_id
            self.client_secret = client_secret
            self.refresh_token = refresh_token
        except Exception as e:
            logging.error(f"Error initializing AmazonAdvertisingAPI: {e}")
            raise


    def create_report_request(self, report_type, start_date, end_date, report_config):
        """
        Creates a report request using the Amazon Advertising API.

        Args:
            report_type (str): Type of the report to be generated.
            start_date (str): Start date for the report (YYYY-MM-DD).
            end_date (str): End date for the report (YYYY-MM-DD).
            report_config (dict): Configuration dictionary for the report.

        Returns:
            str: Report ID for the created report request.

        Raises:
            ValueError: If the report type or configuration is invalid.
            Exception: For other errors encountered during report creation.
        """
        try:
            if not report_type:
                raise ValueError("Report type parameter is missing.")

            if not report_config:
                raise ValueError(f"No configuration found for report: {report_type}")

            url = self.api_urls['report_url']

            payload = {
                "name": report_config['name'],
                "startDate": start_date,
                "endDate": end_date,
                "configuration": {
                    "adProduct": report_config['adProduct'],
                    "groupBy": report_config['groupBy'],
                    "columns": report_config['columns'],
                    "reportTypeId": report_config['reportTypeId'],
                    "timeUnit": report_config['timeUnit'],
                    "format": report_config['format'],
                }
            }
            if 'filters' in report_config and report_config['filters']:
                payload["configuration"]["filters"] = report_config['filters']
                
            logging.info(f"Creating report request with payload: {json.dumps(payload)}")

            headers = {
                'Content-Type': self.content_type,
                'Amazon-Advertising-API-ClientId': self.client_id,
                'Amazon-Advertising-API-Scope': str(self.profile_id),
                'Authorization': f'Bearer {self.access_token}'
            }

            response = requests.post(url, headers=headers, data=json.dumps(payload))
            response.raise_for_status()
            response_json = response.json()

            report_id = response_json.get('reportId')
            if not report_id:
                raise Exception("Report ID not returned in response.")

            logging.info(f"Created Report Request with ID: {report_id}")
            return report_id

        except ValueError as ve:
            logging.error(f"ValueError: {ve}")
            raise

        except requests.RequestException as re:
            logging.error(f"RequestException: {re}")
            raise

        except Exception as e:
            logging.error(f"Error creating report request: {e}")
            raise
        
    def fetch_report_url(self, report_id):
        """
        Fetches the report URL from the Amazon Advertising API.
    
        Args:
            report_id (str): The ID of the report.
    
        Returns:
            list: A list of URLs from which to fetch the report.
    
        Raises:
            Exception: For errors encountered during report URL fetching.
        """
        try:
            url = f"{self.api_urls['report_url']}/{report_id}"
            headers = {
                'Authorization': f'Bearer {self.access_token}',
                'Amazon-Advertising-API-ClientId': self.client_id,
                'Amazon-Advertising-API-Scope': str(self.profile_id)
            }
            max_retries = 15
            retries = 0
            while retries < max_retries:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                response_json = response.json()
                logging.info(f"Attempt {retries + 1}: Response: {response_json}")
                status = response_json.get('status')
                if status == 'COMPLETED':
                    report_urls = response_json.get('url', [])  # Handling multiple URLs
                    if isinstance(report_urls, str):
                        report_urls = [report_urls]
                    elif not isinstance(report_urls, list):
                        logging.error("Expected 'url' to be a list or a single string.")
                        raise TypeError("Expected 'url' to be a list or a single string.")
                    if not report_urls:
                        logging.error("No URLs found in the response.")
                        raise Exception("No URLs found in the response.")
                    logging.info(f"Report URLs: {report_urls}")
                    return report_urls
                elif status == 'FAILURE':
                    logging.error("Report generation failed.")
                    raise Exception("Report generation failed.")

                retries += 1
                logging.info("Sleeping for 3 minutes before retrying.")
                time.sleep(300)

            # If we exhaust retries
            logging.error("Timeout: Report not generated within specified retries.")
            raise TimeoutError("Timeout: Report not generated within specified retries.")

        except requests.RequestException as re:
            logging.error(f"RequestException: {re}")
            raise

        except Exception as e:
            error_message = f"Error fetching report URL: {e}"
            logging.error(error_message)
            raise
        
    def process_report_config(self, report_type, start_date, end_date):
        """
        Processes the configuration for a specific report type.

        Args:
            report_type (str): The type of the report.
            start_date (str): The start date for the report (YYYY-MM-DD).
            end_date (str): The end date for the report (YYYY-MM-DD).
        """
        try:
            # Fetch country-specific configuration
            country_config = get_country_config(self.config,self.country_code)
            self.api_urls = get_api_urls(country_config)
            self.content_type = country_config.get('content_type', '')
            self.destination_output_path_template = get_destination_output_path_template(self.config)
            logging.info(f"destination_output_path_template: {self.destination_output_path_template}")
            # Use common_utils functions for token generation and profile fetching
            self.access_token = generate_access_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                api_urls=self.api_urls
            )
            logging.info(f"Generated Access Token: {self.access_token}")

            self.profile_id = fetch_profile_id(
                access_token=self.access_token,
                client_id=self.client_id,
                api_urls=self.api_urls,
                country = self.country_code
            )
            logging.info(f"Fetched Profile ID: {self.profile_id}")

            # Report configuration
            reports_config = self.config.get('fetcher_parameters', {})
            report_config_key = next(((k, v) for k, v in reports_config.items() if k.lower() == report_type), (None, None))
            # Unpack the results
            key, value = report_config_key
            key_config_path = f"{'/'.join(self.config_path.split('/')[:-1])}/{value}"
            logging.info(f"key_config_path:{key_config_path}")
            report_config = load_configs(key_config_path)
            logging.info(f"report_config_value:{report_config}")
            start_date_iso = format_date(start_date)
            end_date_iso = format_date(end_date)
            end_date_obj = datetime.strptime(end_date_iso, '%Y-%m-%d').date()
            try:
                report_id = self.create_report_request(key, start_date_iso, end_date_iso, report_config)
                report_urls = self.fetch_report_url(report_id)
                logging.info(f"Report URLs: {report_urls}")
                # Assuming that report_urls is a list and we process each URL
                for url in report_urls:
                    fetch_report_data(url, key, self.bucket_nm, self.destination_output_path_template, self.country_code, end_date_obj)
            except Exception as e:
                logging.error(f"An error occurred for report {key}: {e}")
                raise

        except Exception as e:
            logging.error(f"Error processing report config: {e}")
            raise