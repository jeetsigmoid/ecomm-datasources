from utils.common import Task
import requests
from requests_auth_aws_sigv4 import AWSSigV4
import boto3
import json
from time import sleep
from datetime import datetime, timedelta
import pandas as pd
import io


class AWSConnection(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.s3_bucket_name = f"sanofi-chc-emea-datascience-ecommerce-{self.env.lower()}"
        self.root_folder = "de"
        self.session, self.access_key, self.secret_key, self.session_token = self.get_aws_session_credentials()
        self.client = self.create_s3_client()
        self.resource = self.create_s3_resource()
        

    def get_aws_config_parameters(self):
        aws_parameters = {
            "US": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_US"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_US"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_US"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'us-east-1',
                'base_url' : 'https://sellingpartnerapi-na.amazon.com',
                'marketplaceIds' : ['ATVPDKIKX0DER']
            },
            "JP": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_JP"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_JP"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_JP"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'us-west-2',
                'base_url' : 'https://sellingpartnerapi-fe.amazon.com',
                'marketplaceIds' : ['A1VC38T7YXB528']
            },
            "UK": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_UK"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_UK"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_UK"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'eu-west-1',
                'base_url' : 'https://sellingpartnerapi-eu.amazon.com',
                'marketplaceIds' : ['A1F83G8C2ARO7P']
            },
            "MX": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_MX"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_MX"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_MX"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'us-east-1',
                'base_url' : 'https://sellingpartnerapi-na.amazon.com',
                'marketplaceIds' : ['A1AM78C64UM0Y8']
            },
            "IT": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_IT"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_IT"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_IT"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'eu-west-1',
                'base_url' : 'https://sellingpartnerapi-eu.amazon.com',
                'marketplaceIds' : ['APJ6JRA9NG5V4']
            },
            "ES": {
                'access_token_request_body' : {
                    'client_id' : self._get_env_var("MWAA_AVC_CLIENT_ID_ES"),          
                    'client_secret' : self._get_env_var("MWAA_AVC_CLIENT_SECRET_ES"),  
                    'refresh_token' : self._get_env_var("MWAA_AVC_REFRESH_TOKEN_ES"), 
                    'grant_type' : 'refresh_token',
                },
                'report_service' : 'execute-api',
                'region' : 'eu-west-1',
                'base_url' : 'https://sellingpartnerapi-eu.amazon.com',
                'marketplaceIds' : ['A1RKKUPIHCS9HS']
            }
        }
        return aws_parameters[self.country_code]


    def get_aws_session_credentials(self, region_name='us-west-1'):
        """
        Provides AWS session credentials - access key, secret key, and token

        Args:
            region_name: string

        Returns:
            session: boto3.Session
            access_key: string
            secret_key: string
            token: string
        """
        session = boto3.session.Session(region_name='us-west-1')
        credentials = session.get_credentials()
        credentials = credentials.get_frozen_credentials()
        return session, credentials.access_key, credentials.secret_key, credentials.token


    def get_access_token(self, end_point, request_body):
        """
        Generate access token

        Args:
            end_point: string
            request_body: dict

        Returns:
            access_token: string
        """
        headers = {'Content-Type': 'application/x-www-form-urlencoded'}
        response = requests.post(end_point, data=request_body, headers=headers)
        return response.json()['access_token']


    def call_aws_rest_api(self, method, service, region, access_token, endpoint, body, access_key, secret_key,
                          session_token):
        """
        Make a request to AWS service API endpoint and return response

        Args:
            method: string
            service: string
            region: string
            access_token: string
            endpoint: string
            body: string
            access_key: string
            secret_key: string
            session_token: string

        Returns:
            response: dict
        """
        aws_auth = AWSSigV4(service,region=region, aws_access_key_id =access_key, aws_secret_access_key=secret_key, aws_session_token=session_token)

        headers = {
            'Content-Type': 'application/json',
            'x-amz-access-token': access_token
        }
        response = requests.request(method, endpoint, json=body, auth=aws_auth, headers=headers)

        return json.loads(response.text)


    def api_call_with_delay(self, method, service, region, access_token, endpoint, body, access_key,
                            secret_key, session_token):
        """
        Make an API call with a delay of 10 seconds until the error for request limit gets eliminated

        Args:
            method: string
            service: string
            region: string
            access_token: string
            endpoint: string
            body: string
            access_key: string
            secret_key: string
            session_token: string

        Returns:
            response: dict
        """
        delay = 10  # delay with 10 seconds
        while True:
            response = self.call_aws_rest_api(method, service, region, access_token, endpoint, body, access_key,
                                              secret_key, session_token)
            if 'errors' in response.keys() and response['errors'][0]['code'] == 'QuotaExceeded':
                sleep(delay)
            elif 'errors' in response.keys():
                raise Exception(response)
            else:
                return response
            
    
    def create_s3_client(self):
        """
        Creates an S3 client instance

        Returns:
            response: S3.Client
        """
        s3_client = self.session.client(
            service_name='s3',
            region_name='eu-west-1',
        )
        return s3_client
    

    def create_s3_resource(self):
        """
        Creates an S3 resource

        Returns:
            response: S3.Resource
        """
        s3_resource = self.session.resource('s3')
        s3_bucket_as_resource = s3_resource.Bucket(self.s3_bucket_name)
        return s3_bucket_as_resource
    

    def get_all_available_dates_for_one_table_in_s3(self, source_name, table_name):
        """
        Fetches a list of the dates ("%Y-%m-%d") all the csv & parquet files present in 
        the S3 bucket for a specific table

        Args:
            source_name: string
            table_name: string

        Returns:
            response: List[string]
        """
        date_list = [file.key.split('.')[0].split('_')[-1] for file in self.resource.objects.all() 
                            if file.key.startswith(f'{self.root_folder}/{source_name}/{self.country_code}/{table_name}/') 
                            and (file.key.endswith('.csv') or file.key.endswith('.parquet'))]
        return date_list
    


    def get_all_available_file_names_for_one_table_in_s3(self, source_name, table_name, debug=False, input_folder=False):

        """
        Fetches a list of the dates ("%Y-%m-%d") all the csv & parquet files present in 
        the S3 bucket for a specific table

        Args:
            source_name: string
            table_name: string

        Returns:
            response: List[string]
        """
        root_folder = "de_input" if input_folder else self.root_folder
        file_list = ['.'.join(file.key.split('.')[:-1]).split('/')[-1] for file in self.resource.objects.all() 
                            if file.key.startswith(f'{root_folder}/{source_name}/{self.country_code}/{table_name}/') 
                            and (file.key.endswith('.csv') or file.key.endswith('.parquet') or file.key.endswith('.xlsx'))]
        
        if debug:
            print(f'Checking {self.root_folder}/{source_name}/{self.country_code}/{table_name}/')
            print(f'Found {file_list}')
        
        return file_list
    

    def adequate_date_list_for_requests(self, date_list):
        """
        It creates a pair of dates for each date present in the list. This is necessary
        since the endpoints require both start and end date.

        params
        date_list: List[string]

        return
        date_list_2d: List[List[string]]
        """
        if self.pipeline_config['create_report_option']['reportOptions']['reportPeriod'] == 'DAY':
            date_list_2d = [[d, d] for d in date_list]
        if self.pipeline_config['create_report_option']['reportOptions']['reportPeriod'] == 'WEEK':
            date_list_2d = [[(datetime.strptime(d, "%Y-%m-%d") - timedelta(days=6)).strftime("%Y-%m-%d"), d] for d in date_list]
        return date_list_2d    


    def move_previous_csv_to_processed_folder(self, date_list, source_name, table_name):
        """
        Transforms CSV files (only the ones that are not present in date_list) 
        into Parquet and moves them to /processed folder.

        Args:
            date_list: List[string]
            source_name: string
            table_name: string
        """
        file_list = [file.key for file in self.resource.objects.all() 
                            if not file.key.startswith(f'{self.root_folder}/{source_name}/{self.country_code}/{table_name}/processed') 
                            and file.key.startswith(f'{self.root_folder}/{source_name}/{self.country_code}/{table_name}/') 
                            and file.key.split('.')[0].split('_')[-1] not in date_list
                            and file.key.endswith('.csv')]
        
        print(file_list)
        
        if len(file_list) != 0:
            for file_to_move in file_list:
                
                obj = self.client.get_object(Bucket=self.s3_bucket_name, Key=file_to_move)['Body'].read()
                df = pd.read_csv(io.BytesIO(obj))
                split_path = file_to_move.split("/")
                #csv_name = split_path.pop(-1).split('.')[0]
                csv_name = '.'.join(split_path.pop(-1).split('.')[:-1])

                self.client.put_object(
                    Body=bytes(df.to_parquet()),
                    Bucket=self.s3_bucket_name,
                    Key=f'{self.root_folder}/{source_name}/{self.country_code}/{table_name}/processed/{csv_name}.parquet',
                )

                self.client.delete_object(
                    Bucket=self.s3_bucket_name,
                    Key=file_to_move
                )


    def copy_dataframe_as_csv_into_s3_bucket(self, df, date_range, source_name, table_name, sort_cols=True):
        """
        Sorts DataFrame columns in ascending order, converts df into CSV and
        uploads CSV into S3 bucket.

        Args:
            df: DataFrame
            date_range: List[string]
            source_name: string
            table_name: string
        """
        if sort_cols:
            df = df[sorted(df.columns)]
        timestamp = date_range[1]
        self.client.put_object(
            Body=bytes(df.to_csv(index=False), encoding='utf-8'),
            Bucket=self.s3_bucket_name,
            Key=f'{self.root_folder}/{source_name}/{self.country_code}/{table_name}/{table_name}_{timestamp}.csv',
        )  


    def get_most_recent_flat_file_in_bytes(self, custom_path=None):
        path_to_search = custom_path or f"de_input/{self.table_name}/{self.country_code}/{self.table_name}_"
        file_list = [file.key for file in self.resource.objects.all() 
                            if file.key.startswith(path_to_search) 
                            and file.key.split(".")[-1] in ['xlsx', 'csv']]
        if len(file_list) == 0:
            raise Exception(f"There are no raw flat files to be pre-processed for the table {self.table_name}")
        else:
            file_to_move = max(file_list)
            print(file_to_move)
        file_name = file_to_move.split("/")[-1].split(".")[0]
        obj = self.client.get_object(Bucket=self.s3_bucket_name, Key=file_to_move)['Body'].read()
        bytes_obj = io.BytesIO(obj)
        file_date = file_to_move.split("/")[-1].split(".")[0].split("_")[-1]
        return bytes_obj, file_date, file_name

        
    def launch(self):
        return 
