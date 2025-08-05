import yaml
import boto3
import logging
import pkg_resources 
import os

"""
Report Configuration Utility

Created Date: 2024-08-13
Version: 1.0.0
Created By: Anamika
Purpose: This script provides utility functions to load configuration from S3 and retrieve specific configuration details. It is designed to handle various configurations including credentials, API URLs, S3 bucket details, and output path templates.
Functionality:
    - `load_config`: Loads a configuration file from an S3 bucket and returns it as a dictionary.
    - `get_credentials`: Retrieves credentials from the configuration.
    - `get_api_urls`: Retrieves API URLs from the configuration.
    - `get_s3_bucket_name`: Retrieves the name of the S3 bucket.
    - `get_s3_output_path_template`: Retrieves the S3 output path template from the configuration.
    - `get_fetcher_parameters`: Retrieves fetcher parameters from the configuration.
Dependencies:
    - yaml: For parsing YAML files.
    - boto3: For interacting with AWS S3.
    - logging: For logging operations and errors.

History:
    - 2024-08-13: Added error handling for all functions to manage exceptions and log errors.
    - 2024-08-13: Updated docstrings to include detailed descriptions for each function.
"""

def load_config(bucket, config_key):
    """
    Load the configuration from an S3 bucket.

    Description:
        Retrieves a YAML configuration file from the specified S3 bucket and key. Returns the configuration data as a dictionary.

    Args:
        bucket (str): The name of the S3 bucket.
        config_key (str): The key (path) of the configuration file in the S3 bucket.

    Returns:
        dict: The configuration data loaded from the S3 bucket.

    Raises:
        Exception: If there is an error in loading the config file from S3.
    """
    try:
        s3 = boto3.client('s3')
        response = s3.get_object(Bucket=bucket, Key=config_key)
        config_content = response['Body'].read().decode('utf-8')
        return yaml.safe_load(config_content)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        raise

def load_configs(config_path):
    """
    Load the configuration from a YAML file packaged within the wheel.
    :param config_path: The relative path to the config.yml file within the package.
    :return: Configuration dictionary.
    """
    try:
        # Get the current module's directory using __file__
        module_path = os.path.dirname(__file__)  # Get the directory of the current module

        # Trim the path to the site-packages level
        site_packages_path = os.path.dirname(module_path)  # Go up one level
        
        # Append your custom path or use the provided config_path
        custom_config_path = os.path.join(site_packages_path, config_path)

        # Load the YAML config
        with open(custom_config_path, 'r') as config_file:
            config = yaml.safe_load(config_file)

        return config
    except Exception as e:
        raise RuntimeError(f"Failed to load config file: {e}")

def get_credentials(config):
    """
    Retrieve credentials from the configuration.

    Description:
        Fetches the 'credentials' section from the configuration dictionary.

    Returns:
        dict: A dictionary containing credential information.
    
    Raises:
        Exception: If there is an error in retrieving the credentials.
    """
    try:
        return config.get('credentials', {})
    except Exception as e:
        logging.error(f"Failed to retrieve credentials: {e}")
        raise

def get_api_urls(config):
    """
    Retrieve API URLs from the configuration.

    Description:
        Fetches the 'api_urls' section from the configuration dictionary.

    Returns:
        dict: A dictionary containing API URLs.

    Raises:
        Exception: If there is an error in retrieving the API URLs.
    """
    try:
        return config.get('api_urls', {})
    except Exception as e:
        logging.error(f"Failed to retrieve API URLs: {e}")
        raise

def get_destination_bucket_name(config):
    """
    Get the name of the destination bucket.

    Description:
        Retrieves the name of the destination bucket from the instance variable.

    Returns:
        str: The name of the destination bucket.

    Raises:
        Exception: If there is an error in retrieving the destination bucket name.
    """
    try:
        return config.get('destination_bucket', '')
    except Exception as e:
        logging.error(f"Failed to retrieve destination bucket name: {e}")
        raise

def get_destination_output_path_template(config):
    """
    Retrieve the destination output path template from the configuration.

    Description:
        Fetches the 'destination_output_path_template' from the configuration dictionary.

    Returns:
        str: The destination output path template.

    Raises:
        Exception: If there is an error in retrieving the S3 output path template.
    """
    try:
        return config.get('destination_output_path', '')
    except Exception as e:
        logging.error(f"Failed to retrieve S3 output path template: {e}")
        raise
    
def get_country_config(config,country_code):
    """
    Retrieves the configuration for the specified country from the global config.
    
    Returns:
        dict: Configuration dictionary for the specified country.
    """
    country_config = config.get('country_code', {}).get(country_code)
    if not country_config:
        raise ValueError(f"No configuration found for country code: {country_code}")
    return country_config