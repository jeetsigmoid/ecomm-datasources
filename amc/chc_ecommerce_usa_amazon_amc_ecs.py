#backfilling

from datetime import datetime
import time
import json
import requests
import logging
from utils.config_manager import load_configs, get_api_urls, get_destination_output_path_template, get_country_config
from utils.common_utils import generate_access_token, fetch_profile_id, format_date
from utils.report_processor import upload_to_destination
import pandas as pd
import boto3
import os

"""
Amazon Marketing Cloud API Report Processing Utility

Created Date: 2024-08-13
Version: 1.0.0
Created By: Deepak & Shubham
Purpose: This script provides functions to create, fetch, process, and upload Amazon Marketing Cloud reports.

Functionality:
    - `AmazonMarketingCloudAPI.__init__`: Initializes the API client with country code, bucket name, and config.
    - `AmazonMarketingCloudAPI.get_country_config`: Retrieves the configuration for the specified country.
    - `AmazonMarketingCloudAPI.create_report_request`: Creates a report request using the Amazon Marketing Cloud API.
    - `AmazonMarketingCloudAPI.fetch_report_url`: Fetches the URL(s) of the generated report.
    - `AmazonMarketingCloudAPI.process_report_config`: Processes the configuration for generating and handling reports.

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

class AmazonMarketingCloudAPI:
    def __init__(self, country_code,config,bucket_nm, config_path, client_id, client_secret, refresh_token):
        """
        Initializes the AmazonAdvertisingAPI class with the given parameters.

        Args:
            country_code (str): Country code for configuration.
            env (str): Environment.
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
            # self.wf_status = pd.DataFrame(columns=['date', 'report_type', 'workflowID', 'status'])
            self.wf_status = {}
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

            url = self.api_urls['report_url']+'/amc/reporting/'+self.instance_id+'/workflowExecutions'
            
            payload = json.dumps({
                "workflow": {
                "sqlQuery":report_config['query']
                },
                    "timeWindowType": self.time_window_type,
                    "timeWindowStart": f"{start_date}T00:00:00",
                    "timeWindowEnd": f"{end_date}T00:00:00",
                    "ignoreDataGaps": "true",
                    "workflowExecutionTimeoutSeconds": "86400"
                })
            logging.info(f"Creating report request with payload: {payload}")

            headers = {"Amazon-Advertising-API-ClientId": self.client_id, 
                   "Content-Type": self.content_type,
                   "Authorization": f'Bearer {self.access_token}',
                   "Amazon-Advertising-API-MarketplaceId":self.market_place_ids,
                   "Amazon-Advertising-API-AdvertiserId": self.entity_id
                   }

            response = requests.post(url, headers=headers, data=payload)
            response.raise_for_status()
            response_json = response.json()
      
            report_id = response_json.get('workflowExecutionId')
            if not report_id:
                raise Exception("Report ID not returned in response.")

            logging.info(f"Created Report Request with ID: {report_id}")
            logging.info("1.create_report_request completed")
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
            logging.info("2.fetch_report_url started")
            
            url = self.api_urls['report_url']+'/amc/reporting/'+self.instance_id+'/workflowExecutions/'+report_id+'/downloadUrls'
            status_url=self.api_urls['report_url']+'/amc/reporting/'+self.instance_id+'/workflowExecutions/'+report_id
           
            headers = {
                    'Amazon-Advertising-API-ClientId': self.client_id,
                    'Authorization': f'Bearer {self.access_token}',
                    'Content-type': self.content_type,
                    'Amazon-Advertising-API-MarketplaceId': self.market_place_ids,
                    'Amazon-Advertising-API-AdvertiserId': self.entity_id
                    }
            max_retries = 10
            retries = 0
            while retries < max_retries:
                response = requests.get(status_url, headers=headers)
                response.raise_for_status()
                response_json = response.json()
            
                logging.info(f"Attempt {retries + 1}")

                status = response_json.get('status')
                logging.info(status)
                if status == 'SUCCEEDED':
                    report_urls = url  
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

                elif status == 'FAILURE' or status == 'REJECTED':
                    logging.info(f"Status: {status}")
                    logging.info(f"Report generation failed: {report_id}")
                    self.wf_status= {
                        'workflowID': report_id,
                        'date':  None, # Initialize, to be updated later
                        'report_type': None, # Initialize, to be updated later
                        'status': status 
                    }
                    return ['']

                retries += 1
                logging.info("Sleeping for 3 minutes before retrying.")
                time.sleep(180)

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
            
            # Initialize API URLs based on country configuration
            self.api_urls = get_api_urls(country_config)
            self.market_place_ids = country_config['marketPlaceIds']
            self.instance_id = country_config['instance_id']
            logging.info(f"instance_id:{self.instance_id}")
            self.time_window_type = country_config['timeWindowType']
            self.entity_id = country_config['entity_id']
            logging.info(f"entity_id:{self.entity_id}")
            self.content_type = country_config['content_type']
            self.destination_path_template = get_destination_output_path_template(self.config)
            logging.info(f"destination_output_path_template: {self.destination_path_template}")
            # Use common_utils functions for token generation and profile fetching
            self.access_token = generate_access_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                api_urls=self.api_urls
            )
            logging.info(f"Generated Access Token: {self.access_token}")

            # self.profile_id = fetch_profile_id(
            #     access_token=self.access_token,
            #     client_id=self.client_id,
            #     api_urls=self.api_urls,
            #     country = self.country_code
            # )
            # logging.info(f"Fetched Profile ID: {self.profile_id}")

            # Report configuration
            reports_config = self.config.get('reports', {})
            report_config_key = next(((k, v) for k, v in reports_config.items() if k.lower() == report_type), (None, None))
            # Unpack the results
            key, value = report_config_key
            key_config_path = f"{'/'.join(self.config_path.split('/')[:-1])}/{value}"
            logging.info(f"key_config_path:{key_config_path}")
            report_config = load_configs(key_config_path)

            start_date_iso = format_date(start_date)
            end_date_iso = format_date(end_date)

            try:
                report_id = self.create_report_request(report_type, start_date_iso, end_date_iso, report_config)
                logging.info(f"3. report_id: {report_id}")
                report_urls = self.fetch_report_url(report_id)[0]
                if report_urls !='':
                    logging.info(f"Report URLs: {report_urls}")
                
                    logging.info(f"------------1.Bucket: {self.bucket_nm}------------")
                    logging.info(f"------------1.Path: {self.destination_path_template}------------")

                    headers = {
                        'Amazon-Advertising-API-ClientId': self.client_id,
                        'Authorization': f'Bearer {self.access_token}',
                        'Content-type': self.content_type,
                        'Amazon-Advertising-API-MarketplaceId': self.market_place_ids,
                        'Amazon-Advertising-API-AdvertiserId': self.entity_id
                        }
                    #Download the report data
                    response = requests.get(report_urls, headers=headers)
                    response.raise_for_status()

                    json_data = json.loads(response.text)
                    download_urls = json_data.get("downloadUrls")[0]

                    response_url=requests.get(download_urls)
                    
                    local_filename = f'/tmp/{report_type}.csv'

                    with open(local_filename, 'wb') as f:
                        f.write(response_url.content)
                    
                    start_date_obj = datetime.strptime(start_date_iso, '%Y-%m-%d').date()
                    # self.destination_output_path_template=f'de/amc_archive/{self.country_code}/{report_type}/{report_type}_{start_date}.csv'
                    upload_to_destination(local_filename, key, self.bucket_nm, self.destination_path_template, self.country_code, start_date_obj)
                    logging.info(f"File successfully uploaded to s3://{self.bucket_nm}/{self.destination_path_template}")

                else:
                    # delete log where workflow succeeded
                    self.wf_status['date']=start_date
                    self.wf_status['report_type'] = report_type
                    logging.info(f"--- Workflow Status: {self.wf_status} ---")
                    workflow_log = pd.read_csv('workflow_status.csv')
                    workflow_log = pd.concat([workflow_log, pd.DataFrame([self.wf_status])], ignore_index=True)
                    workflow_log.to_csv('workflow_status.csv', index=False)
                    log_destination= '/'.join(self.destination_path_template.split('/')[:-1])

                    year = datetime.now().strftime("%Y")
                    month = datetime.now().strftime("%m")
                    day = datetime.now().strftime("%d")
                    start_date_obj = datetime.strptime(start_date_iso, '%Y-%m-%d').date()

                    if not workflow_log.empty:
                        log_destination = f'{log_destination}/log/log_{day}{month}{year}.csv'
                        logging.info(f"--------log destination: s3://{self.bucket_nm}/{log_destination}")
                        logging.info(f"log: {workflow_log}")
                        upload_to_destination('workflow_status.csv', key, self.bucket_nm, log_destination, self.country_code, start_date_obj)
                        logging.info(f"Failure Log uploaded to s3://{self.bucket_nm}/{log_destination}")
                      
            except Exception as e:
                logging.error(f"An error occurred for report {report_type}: {e}")
                raise

            logging.info("3.process_report_config completed")

        except Exception as e:
            logging.error(f"Error processing report config: {e}")
            raise