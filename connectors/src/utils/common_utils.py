import requests
import pandas as pd
import logging
from dateutil import parser  # Ensure you have dateutil installed for parsing dates
from datetime import datetime, timedelta
import boto3
import os

"""
Common Utilities for Amazon Advertising Script

Created Date: 2024-07-15
Version: 1.0.0
Created By: Anamika
Purpose: This module provides utility functions for interacting with the Amazon Advertising API, including generating access tokens, fetching profile IDs, and formatting dates.

Functionality:
- `generate_access_token`: Generates an access token using client credentials and a refresh token.
- `fetch_profile_id`: Retrieves the profile ID for advertising operations using the access token.
- `format_date`: Formats date strings into a standard 'YYYY-MM-DD' format for consistency in API requests.

Dependencies:
- requests: For making HTTP requests to the API.
- logging: For logging information and errors.
- dateutil.parser: For parsing and formatting date strings.
"""

def generate_access_token(client_id, client_secret, refresh_token, api_urls):
    """
    Generate an access token using the provided refresh token.

    Args:
        client_id (str): The client ID for the Amazon Advertising API.
        client_secret (str): The client secret for the Amazon Advertising API.
        refresh_token (str): The refresh token used to obtain a new access token.
        api_urls (dict): Dictionary containing the URLs for the API endpoints.

    Returns:
        str: The access token.

    Raises:
        Exception: If there is an error generating the access token or if the token is not returned.
    """
    try:
        # URL for access token generation
        url = api_urls['access_token_generation']

        # Payload for the POST request
        payload = {
            'grant_type': 'refresh_token',
            'client_id': client_id,
            'client_secret': client_secret,
            'refresh_token': refresh_token
        }

        # Headers for the POST request
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}

        # Send the POST request
        response = requests.post(url, data=payload, headers=headers)
        response.raise_for_status()  # Raise an error for bad status codes

        # Extract access token from the response
        access_token = response.json().get('access_token')
        logging.info(f"Generated Access Token: {access_token}")
        return access_token
    except Exception as e:
        logging.error(f"Error generating access token: {e}")
        raise

def fetch_profile_id(access_token, client_id, api_urls, country=None):
    """
    Fetch the profile ID using the provided access token.

    Args:
        access_token (str): The access token used to authenticate the API request.
        client_id (str): The client ID for the Amazon Advertising API.
        api_urls (dict): Dictionary containing the URLs for the API endpoints.

    Returns:
        str: The profile ID.

    Raises:
        Exception: If there is an error fetching the profile ID or if no profiles are found.
    """
    try:
        profiles_url = api_urls['profile_fetch']
        headers = {
            'Authorization': f'Bearer {access_token}',
            'Amazon-Advertising-API-ClientId': client_id,
        }
        response = requests.get(profiles_url, headers=headers)
        response.raise_for_status()
        profiles = response.json()
        if not profiles:
            raise Exception("No profiles found.")

        if country:
            if country == 'GB':
                country='UK'

            for profile in profiles:
                if (profile.get("countryCode") == country and profile.get("accountInfo", {}).get("type") == "vendor"):
                    return profile.get("profileId")
            raise Exception(f"No profile id found for country :{country}")
        else:
            return profiles[0]['profileId']
    except Exception as e:
        logging.error(f"Error fetching profile ID: {e}")
        raise

def format_date(date_str):
    """
    Format a date string into 'YYYY-MM-DD' format.

    Args:
        date_str (str): The date string to be formatted.

    Returns:
        str: The formatted date string in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If the date string could not be parsed.
    """
    try:
        # Parse the date string using dateutil's parser
        parsed_date = parser.parse(date_str)

        # Format the parsed date to 'YYYY-MM-DD'
        formatted_date = parsed_date.strftime('%Y-%m-%d')

        return formatted_date
    except ValueError as e:
        logging.error(f"Error: Could not parse the date. {e}")
        raise

def make_api_calls_daily(report_start_date, report_end_date):
    """
    Return combination of start_date and end_date with a 1-day interval.
    Each tuple in date_list will have (start_date, end_date).
    """
    
    start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
    end_date = datetime.strptime(report_end_date, "%Y-%m-%d")

    date_list = []
    
    # Iterate while the current start_date is less than or equal to the end_date
    while start_date < end_date:
        next_day = start_date + timedelta(days=1)
        date_list.append((start_date.strftime('%Y-%m-%d'), next_day.strftime('%Y-%m-%d')))
        start_date = next_day
        
    return date_list


def generate_intervals(report_start_date, report_end_date, interval_days=10):
    """
    Generate intervals of a specified number of days (default: 10 days)
    between report_start_date and report_end_date.

    Args:
        report_start_date (str): Start date in 'YYYY-MM-DD' format.
        report_end_date (str): End date in 'YYYY-MM-DD' format.
        interval_days (int): Number of days per interval.

    Returns:
        list: List of tuples representing start and end dates for each interval.
    """
    start_date = datetime.strptime(report_start_date, "%Y-%m-%d")
    end_date = datetime.strptime(report_end_date, "%Y-%m-%d")

    intervals = []

    while start_date <= end_date:
        interval_end = min(start_date + timedelta(days=interval_days - 1), end_date)
        intervals.append((start_date.strftime('%Y-%m-%d'), interval_end.strftime('%Y-%m-%d')))
        start_date = interval_end + timedelta(days=1)  # Start next interval

    return intervals


def get_env_var(var_name):
    try:
        if os.getcwd().startswith('/home/u10'):
            return os.environ.get(var_name)
        else:
            secret_manager = boto3.client("secretsmanager", region_name="eu-west-1")
            return secret_manager.get_secret_value(SecretId=f"PROJECT/chc-ecommerce-analytics-usa/{var_name}")["SecretString"]
    except Exception as e:
        logging.error(f"invalid variable name: {e}")
        

def read_excel_file(file_path, sheet_name, skiprows, usecols):
    """
    Reads a Excel file and returns a pandas DataFrame.
    
    Parameters:
    file_path (str): The path to the Excel file.
    
    Returns:
    pd.DataFrame: The contents of the Excel file as a DataFrame.
    """
    try:
        df = pd.read_excel(file_path, sheet_name=sheet_name, skiprows=skiprows, usecols=usecols)
        
        columns_in_df = df.columns
        
        return df
    
    except Exception as e:
        print(f"An error occurred in read_excel_file: {e}")

def read_csv_file(file_path, skiprows):
    """
    Reads a CSV file and returns a pandas DataFrame.
    
    Args:
        file_path (str): Path to the CSV file.
        skiprows (int or list-like, optional): Line numbers to skip (0-indexed) or number of lines to skip at the start.
    
    Returns:
        pd.DataFrame: DataFrame containing the file content.
    
    Raises:
        Exception: If reading the file fails.
    """
    try:
        logging.info(f"Reading CSV file: {file_path} with skiprows={skiprows}")
        df = pd.read_csv(file_path, skiprows=skiprows)
        logging.info(f"CSV read successfully. Rows: {len(df)}, Columns: {len(df.columns)}")
        return df
    except Exception as e:
        logging.error(f"Failed to read CSV file {file_path}: {e}")
        raise

def rename_columns(dataframe, column_mapping):
    """
    Renames columns in a DataFrame using the provided column mapping.

    Parameters:
    dataframe (pd.DataFrame): The DataFrame whose columns need to be renamed.
    column_mapping_target (dict): A dictionary where keys are current column names
                                   and values are the new column names.

    Returns:
    pd.DataFrame: The DataFrame with renamed columns.
    """
    # Rename columns using the mapping
    dataframe = dataframe.rename(columns=column_mapping)
    return dataframe