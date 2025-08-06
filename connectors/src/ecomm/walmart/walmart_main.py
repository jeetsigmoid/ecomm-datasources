import argparse
import logging
import os
from datetime import datetime, timedelta

# Import the Walmart class
from walmart.chc_ecommerce_walmart_ecs import WalmartReportProcessor
from utils.config_manager import load_configs

# Set logging level
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def main():
    parser = argparse.ArgumentParser(description='Walmart arguments')
    
    # Define all the arguments
    parser.add_argument('--report_type', help='Name of the report endpoint')
    parser.add_argument('--date', help='date')
    parser.add_argument('--country_code', required=True, help='Country code')  
    parser.add_argument('--consumer_id', help='Consumer ID for Walmart')
    parser.add_argument('--consumer_version', help='Consumer version')
    parser.add_argument('--config_path', required=True, help='Path to the config file')
    parser.add_argument('--jar_path', help ='walmart jar_path')
    parser.add_argument('--bucket_nm', help ='bucket name')
    parser.add_argument('--start_date', help ='start date (YYYY-MM-DD)')
    parser.add_argument('--end_date', help ='end date (YYYY-MM-DD)')
    
    # Parse arguments
    args = parser.parse_args()
    
    try:
        # Load configuration from the config file
        config = load_configs(args.config_path)
        logging.info(f"Config loaded: {config}")
        
        # Log environment variables
        for key, value in os.environ.items():
            logging.debug(f"{key}: {value}")
        
        # Initialize WalmartReportProcessor
        walmart_api = WalmartReportProcessor(args.country_code, config, args.consumer_id, args.consumer_version, args.config_path, args.jar_path, args.bucket_nm)

        # Determine the report type
        report_type = args.report_type
        if not report_type:
            logging.error("No report type provided.")
            return
        
        logging.info(f"Processing report type: {report_type}")
        logging.info(f"Processing date: {args.date}")
        logging.info(f"Processing start_date: {args.start_date}")
        logging.info(f"Processing end_date: {args.end_date}")
        # Handle date range processing if start_date and end_date are provided
        if args.start_date != 'None' and args.end_date != 'None':
            try:
                if args.date != 'None':
                    start_date = datetime.strptime(args.date, "%Y-%m-%d")
                    end_date = datetime.strptime(args.date, "%Y-%m-%d")
                else:
                    # Convert start_date and end_date to datetime objects
                    start_date = datetime.strptime(args.start_date, "%Y-%m-%d")
                    end_date = datetime.strptime(args.end_date, "%Y-%m-%d")
                if start_date > end_date:
                    logging.error(f"Start date ({start_date}) cannot be later than end date ({end_date})")
                    return
                    
                # Process the report for each date in the range
                current_date = start_date
                while current_date <= end_date:
                    logging.info(f"Processing report for date: {current_date.strftime('%Y-%m-%d')}")
                    walmart_api.process_report(report_type, current_date.strftime('%Y-%m-%d'))
                    current_date += timedelta(days=1)
            except ValueError as e:
                logging.error(f"Invalid date format: {e}")
                return
        else:
            # If no dates are provided, process the report without dates
            logging.info("No date range provided. Processing without dates.")
            walmart_api.process_report(report_type, None)
        
    except ValueError as ve:
        # Handle missing or invalid value errors
        logging.error(f"ValueError: {ve}")
        raise
    except Exception as e:
        # Catch all other exceptions
        logging.error(f"Error in main execution: {e}")
        raise

if __name__ == "__main__":
    main()