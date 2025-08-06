from datetime import datetime
import time
import json
import requests
import logging
from utils.config_manager import load_configs, get_api_urls, get_destination_output_path_template,get_country_config
from utils.common_utils import generate_access_token,  format_date
from utils.report_processor import fetch_report_data  


# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

class InstaCartReportProcessor:

    def __init__(self, country_code,config,bucket_nm,config_path, client_id, client_secret, refresh_token):
        try:
            logging.info("Initializing InstacartAPI")
            self.client_id = client_id
            self.client_secret = client_secret
            self.refresh_token = refresh_token
            self.country_code = country_code
            self.config = config
            self.bucket_nm = bucket_nm
            self.config_path = config_path

        except Exception as e:
            logging.error(f"Error initializing InstaCartReportProcessor: {e}")
            raise

    def process_report(self, report_type, start_date, end_date):
        """
        Processes the configuration for a specific report type.

        Args:
            start_date (str): The start date for the report (YYYY-MM-DD).
            end_date (str): The end date for the report (YYYY-MM-DD).
        """
        try:
            # Fetch country-specific configuration
            country_config = get_country_config(self.config,self.country_code)
            self.api_urls = get_api_urls(country_config)
            self.content_type = country_config.get("headers",{}).get("content_type", "")
            self.accept = country_config.get("headers",{}).get("accept", "")
            self.sight_config = self.config.get("instacart_sights",{})
            self.destination_output_path_template = get_destination_output_path_template(self.config)
            logging.info(f"destination_output_path_template: {self.destination_output_path_template}")
            
            # generate access token
            self.access_token = generate_access_token(
                client_id=self.client_id,
                client_secret=self.client_secret,
                refresh_token=self.refresh_token,
                api_urls=self.api_urls
            )
            logging.info(f"Generated Access Token: {self.access_token}")

            start_date_iso = format_date(start_date)
            end_date_iso = format_date(end_date)
            end_date_obj = datetime.strptime(end_date_iso, '%Y-%m-%d').date()
            try:
                report_id = self.create_report_request(start_date_iso, end_date_iso)
                report_urls = self.fetch_report_url(report_id)
                logging.info(f"Report URLs: {report_urls}")
                # Assuming that report_urls is a list and we process each URL
                for url in report_urls:
                    fetch_report_data(url, report_type, self.bucket_nm, self.destination_output_path_template, self.country_code, end_date_obj)
            except Exception as e:
                logging.error(f"An error occurred for report {report_type}: {e}")
                raise

        except Exception as e:
            logging.error(f"Error processing report config: {e}")
            raise

    def create_report_request(self, start_date, end_date):
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
            url = self.api_urls['report_url']
            sight_config: self.config["country_code"][self.country_code]["instacart_insights"]

            payload = {
                "date_range":{
                    "start_date": start_date,
                    "end_date": end_date
                },
                "timespan": self.sight_config.get("timespan"),
                "dimensions": self.sight_config.get("dimensions",[]),
                "metrics": self.sight_config.get("metrics",[]),
            }
        
            logging.info(f"Instacart creating report request with payload: {json.dumps(payload)}")

            headers = {
                # 'Accept': self.accept,
                'Content-Type': self.content_type,
                'Authorization': f'Bearer {self.access_token}'
            }

            logging.info(f"Instacart creating report request with headers: {json.dumps(headers)}")

            response = requests.post(url, headers=headers, json=payload)
            response.raise_for_status()
            response_json = response.json()
            report_id = response_json.get('data', {}).get('id')
            if not report_id:
                logging.error("Missing report id in response : {json.dumps(response_json, indent=2)}")
                raise
            logging.info(f"Created Report Request successfully with ID: {report_id}")
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
        """Fetch the report download URL."""
        try:
            logging.info(f"Fetching report for report_id: {report_id}")
            base_url = self.api_urls["report_url"].rsplit('/',1)[0]
            url = f"{base_url}/{report_id}"
            
            headers = {
                'Accept': self.accept,
                'Content-Type': self.content_type,
                'Authorization': f'Bearer {self.access_token}'
            }
            max_retries = 15
            retries = 0
            while retries < max_retries:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                response_json = response.json().get("data",{}).get("attributes",{})
                logging.info(f"Attempt {retries + 1}: Response: {response_json}")
                status = response_json.get('status')
                if status == 'completed':
                    report_urls = response_json.get('s3_location',[])
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