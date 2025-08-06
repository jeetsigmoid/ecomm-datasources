import argparse
import logging
from datetime import datetime, timedelta
from utils.config_manager import load_configs
from instacart.chc_ecommerce_instacart_ecs import InstaCartReportProcessor

"""
Instacart Report Fetcher Script

Created Date: 2025-04-04
Version: 1.0.0
Created By: Utkarsh
Purpose: This script is designed to fetch and process Instacart reports. 
It interacts with the Instacart API to process multiple types of reports, 
allowing users to specify the report type, date range, and country configuration via 
command-line arguments.

Main Functionality:
    - Parses command-line arguments for report type, start, and end dates, and country code.
    - Loads configuration settings.
    - Extracts fetcher parameters for report processing.
    - Initializes the AmazonAdvertisingAPI for API interaction.
    - Processes specified report types, leveraging country-specific configurations.
    - Generates an access token and fetches a profile ID for Amazon Advertising API operations.

Dependencies:
    - argparse
    - logging
    - utils.config_manager (custom utility module)
    - amazon_ads.chc_ecommerce_usa_amzon_ads_ecs (custom API module)

Usage:
    - This script is intended to be run as a command-line utility with the following arguments:
      - --report_type: Specifies the type of report endpoint.
      - --start_date: The start date for the report in YYYY-MM-DD format.
      - --end_date: The end date for the report in YYYY-MM-DD format.
      - --country_code: The country code for fetching country-specific configurations.
"""
# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    """
    Main function for executing the Amazon Advertising Report Fetcher.

    Parses command-line arguments, validates them, initializes the AmazonAdvertisingAPI class,
    and processes the specified report types based on the provided configuration and arguments.
    Handles exceptions to ensure proper error logging and feedback.
    """

    # Set up argument parser for command-line inputs
    parser = argparse.ArgumentParser(
        description='Amazon Advertising Report Fetcher'
    )
    parser.add_argument('--report_type', help='Name of the report endpoint')
    parser.add_argument('--start_date', help='Start date in YYYY-MM-DD format')
    parser.add_argument('--end_date', help='End date in YYYY-MM-DD format')
    parser.add_argument('--country_code', help='Country code')
    parser.add_argument('--client_id', help='Client id')
    parser.add_argument('--client_secret', help='Client secret')
    parser.add_argument('--refresh_token', help='Refresh token')
    parser.add_argument('--config_path', required=True, help='Path to the config file')
    parser.add_argument('--bucket_nm', required=True, help='Bucket name')

    # Parse command-line arguments
    args = parser.parse_args()
   
    try:
        # Validate required arguments
        if not args.start_date or not args.end_date:
            raise ValueError("Start date and end date must be provided.")

        if not args.country_code:
            raise ValueError("Country code must be provided.")

        config = load_configs(args.config_path)
        logging.info(f"Loaded config: {config}")

        # Initialize the AmazonAdvertisingAPI class
        instacart_api = InstaCartReportProcessor(
            args.country_code, config, args.bucket_nm, args.config_path, args.client_id, args.client_secret, args.refresh_token
        )

        report_type = args.report_type

        logging.info(f"Processing Instacart report")
        
        # instacart_api.process_report_config(args.start_date, args.end_date)
        logging.info(f"start_date: {args.start_date}")
        logging.info(f"end_date: {args.end_date}")

        parsed_start_datetime = datetime.fromisoformat(args.start_date)
        start_date_string = parsed_start_datetime.strftime("%Y-%m-%d")

        parsed_end_datetime = datetime.fromisoformat(args.end_date)
        end_date_string = parsed_end_datetime.strftime("%Y-%m-%d")

        logging.info(f"Converted start_date_string: {start_date_string}")
        logging.info(f"Converted end_date_string: {end_date_string}")

        logging.info(f"Processing report for below date range: {args.start_date} - {args.end_date}")
        instacart_api.process_report(args.report_type, args.start_date, args.end_date)
            

    except ValueError as ve:
        # Handle value errors (e.g., missing dates)
        logging.error(f"ValueError: {ve}")
        raise
    except Exception as e:
        # Handle general exceptions
        logging.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()