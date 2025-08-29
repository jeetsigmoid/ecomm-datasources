
from datetime import date
import urllib
import zlib
import json
import os
import requests
from time import sleep
from datetime import datetime, timedelta
from utils.common import Task
import pandas as pd
from snowflake.snowpark.functions import current_timestamp, col, lit
from utils.snowpark_utils import snowflake_connection, read_and_write
from utils.aws_connection import AWSConnection


class AVCIngestionTask(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.common_config = self.common_conf
        self.access_token_endpoint = self.common_config["access_token_endpoint"]
        self.create_report_path = self.common_config["create_report_path"]
        self.get_report_path = self.common_config["get_report_path"]
        self.download_report_path = self.common_config["download_report_path"]
        self.error_to_be_skipped = self.common_config["error_to_be_skipped"]
        self.error_message_to_be_handle = self.common_config["error_message_to_be_handle"]
        self.market_basket_report_error_message = self.common_config["market_basket_report_error_message"]
        self.repeat_purchase_report_error_message = self.common_config["repeat_purchase_report_error_message"]
        self.report_error_message_2 = self.common_config["report_error_message_2"]
        self.large_json_tables = self.common_config["large_json_tables"]
        self.download_path = os.getcwd() + '/' + self.table_name + '.gz'
        self.csv_path = os.getcwd() + '/' + self.table_name + '.csv'
        self.source = "avc"
        self.session = snowflake_connection().sf_connection()
        self.rw = read_and_write(self.session)
        self.aws_connect=AWSConnection()


    def download_file(self, url, download_path):
        """
        Downloads file in chunks to disk.

        Args:
        url: string
        download_path: string
        """
        with requests.get(url, stream=True) as r:
            r.raise_for_status()
            with open(download_path, 'wb') as f:
                for chunk in r.iter_content(chunk_size=8192): 
                    f.write(chunk)


    #  Download report from amazon vendor central and return json response 
    def get_report_from_avc(self, start_date, end_date, aws_config, access_key, secret_key, session_token):
        """
        Download report from amazon vendor central and return json response

        params
        start_date: string
        end_date: string
        aws_config: dict
        access_key: string
        secret_key: string
        session_token: string

        return
        record_json: dict
        """

        print(f"Generating Access Token for start date : {start_date} and end date : {end_date}")
        access_token = self.aws_connect.get_access_token(self.access_token_endpoint, aws_config['access_token_request_body'])

        print(
            f"Creating Report to generate report id for start date : {start_date} and end date : {end_date}"
        )
        
        create_report_endpoint = aws_config["base_url"] + self.create_report_path
        create_report_request_body = {
            "reportType": self.pipeline_config["create_report_option"]["reportType"],
            "reportOptions": self.pipeline_config["create_report_option"]["reportOptions"] if not self.table_name in ["sp_promotion_performance", "sp_coupon_performance"] else dict(),
            "dataStartTime": start_date,
            "dataEndTime": end_date,
            "marketplaceIds": aws_config["marketplaceIds"],
        }

        if self.table_name in ["sp_promotion_performance", "sp_coupon_performance"]:
            del create_report_request_body["dataStartTime"]
            del create_report_request_body["dataEndTime"]
            create_report_request_body["reportOptions"]["campaignStartDateFrom"] = start_date
            create_report_request_body["reportOptions"]["campaignStartDateTo"] = end_date
        
        create_report_response = self.aws_connect.api_call_with_delay(
            "POST",
            aws_config['report_service'],
            aws_config['region'],
            access_token,
            create_report_endpoint,
            create_report_request_body, 
            access_key, 
            secret_key ,
            session_token
        )

        report_id = create_report_response["reportId"]
        
        print(f"Generating report document id for start date : {start_date} and end date : {end_date}")
        get_report_endpoint = aws_config['base_url'] + self.get_report_path + report_id
        get_report_response = self.aws_connect.api_call_with_delay(
            "GET",
            aws_config['report_service'],
            aws_config['region'],
            access_token ,
            get_report_endpoint,
            None, 
            access_key, 
            secret_key, 
            session_token
        )

        status = get_report_response["processingStatus"]
        print(get_report_response)
        
        while status == "IN_QUEUE" or status == "IN_PROGRESS":
            # Iterate and make request to get_report_endpoint until the processing status changes to other than IN_QUEUE and IN_PROGRESS
            sleep(5)
            get_report_response = self.aws_connect.api_call_with_delay(
                "GET",
                aws_config['report_service'],
                aws_config['region'],
                access_token,
                get_report_endpoint,
                None,
                access_key,
                secret_key,
                session_token
            )
            print(get_report_response)
            status = get_report_response["processingStatus"]

        report_document_id = get_report_response["reportDocumentId"]
        
        print(
            f"Generating download url and creating dataframe for start date : {start_date} and end date : {end_date}"
        )

        download_report_endpoint = aws_config['base_url'] + self.download_report_path + report_document_id

        download_report_response = self.aws_connect.api_call_with_delay(
            "GET",
            aws_config['report_service'],
            aws_config['region'],
            access_token,
            download_report_endpoint,
            None,
            access_key,
            secret_key,
            session_token
        )
        print(download_report_response)
        download_url = download_report_response["url"]

        if self.table_name in self.large_json_tables:
            self.download_file(download_url, self.download_path)
        else:
            record_json = json.loads(
                zlib.decompress(
                    urllib.request.urlopen(download_url).read(), 
                    16 + zlib.MAX_WBITS
                ).decode("utf-8")
            )
            return record_json


    def raw_dataframe_cleaning(self, raw_df, date_range):
        """
        Changing data type for date columns and converting column name from lowercase to Uppercase

        Args:
        raw_df: DataFrame
        date_range: List[string]

        Returns:
        raw_df: DataFrame
        """
        print("started raw_dataframe_cleaning")
        raw_df_columns = raw_df.columns
        for column in raw_df_columns:
            if self.table_name in ["sp_promotion_performance", "sp_coupon_performance"]:
                raw_df = raw_df.withColumnRenamed(column,column.upper())
                if 'time' in column.lower():
                    raw_df = raw_df.withColumn(column.upper(),col(column.upper()).cast('timestamp'))
            elif self.table_name == "sp_search_terms":
                if 'date' in column.lower():
                    raw_df = raw_df.withColumnRenamed(column,column.upper())
                    raw_df = raw_df.withColumn(column.upper(),col(column.upper()).cast('date'))
                else:
                    raw_df = raw_df.withColumnRenamed(column,column.upper())
                    rename_column=column.replace(' ','_')
                    raw_df = raw_df.withColumnRenamed(column,rename_column)\
                                        .withColumnRenamed(rename_column,rename_column.upper())
            else:
                if ".amount" in column.lower():
                    # if .amount suffix is present in column name then remove the suffix and convert it to uppercase
                    rename_column = column.split(".")[0] + '"'
                    if rename_column in raw_df_columns:
                        raw_df = raw_df.drop(rename_column.replace('"', "")).drop(rename_column)
                    raw_df = raw_df.withColumnRenamed(column, rename_column).withColumnRenamed(
                        rename_column, rename_column.upper()
                    )

                elif ".currencycode" in column.lower():
                    # if .currencycode suffix is present in column name then replace (.) with (_) and convert it to uppercase
                    rename_column = column.replace(".", "_")
                    raw_df = raw_df.withColumnRenamed(column, rename_column).withColumnRenamed(
                        rename_column, rename_column.upper()
                    )

                elif "date" in column.lower():
                    # if date string is present in column name then cast it to date format and convert the column name to uppercase
                    raw_df = raw_df.withColumnRenamed(column, column.upper())
                    raw_df = raw_df.withColumn(column.upper(), col(column.upper()).cast("date"))

                else:
                    # else convert the column name to uppercase
                    raw_df = raw_df.withColumnRenamed(column, column.upper())

        raw_df = raw_df.withColumn("COUNTRY_CODE", lit(self.country_code))
        raw_df = raw_df.withColumn("LAST_UPDATED_TIMESTAMP", current_timestamp())

        if self.table_name == "sp_promotion_performance":
            raw_df = raw_df.replace({'null': None}, subset=['FUNDINGAGREEMENTID'])

        if self.table_name == "sp_coupon_performance":
            for column in ["TOTALBUDGETSPENT", "TOTALBUDGETREMAINING", "TOTALBUDGET"]:
                if column not in raw_df_columns:
                    raw_df = raw_df.withColumn(column, lit(0))

        if self.table_name == "sp_search_terms":
            startdate = date_range[0]
            enddate = date_range[1]
            raw_df = raw_df.withColumn('STARTDATE',lit(startdate))\
                                        .withColumn("STARTDATE",col("STARTDATE").cast('date'))
            raw_df = raw_df.withColumn('ENDDATE',lit(enddate))\
                                        .withColumn("ENDDATE",col("ENDDATE").cast('date'))
            
            df_catalog = self.rw.read_from_sf(self.pipeline_config['catalog_details'])
            df_catalog = df_catalog.filter(col("COUNTRY_CODE") == self.country_code)

            catalog_raw = df_catalog.select('asin')
            temp_df = raw_df.join(catalog_raw, raw_df['clickedasin'] == catalog_raw['asin'], 'inner')      
            raw_df = temp_df.drop('asin')

        return raw_df


    def generate_raw_dataframe(self, date_range, aws_config, access_key, secret_key, session_token):
        """
        Generating raw dataframe for given report type

        params
        date_range: tuple
        aws_config: dict
        access_key: string
        secret_key: string
        session_token: string

        return
        raw_df: dataframe
        """

        print(f"Fetching report from {date_range[0]}  to {date_range[1]}")
        record_json = self.get_report_from_avc(
            date_range[0], date_range[1], aws_config, access_key, secret_key, session_token
        )

        if self.table_name in self.large_json_tables:
            command = f"gunzip -c {self.download_path} {self.pipeline_config['jq_command']} {self.csv_path}"
            os.system(command)
            record_df = pd.read_csv(self.csv_path, header=None, names=self.pipeline_config["jq_csv_column_names"])
            os.remove(self.download_path)
            os.remove(self.csv_path)
            
        else:
            if ("errorDetails" in record_json.keys() and (record_json["errorDetails"] == self.error_to_be_skipped or record_json["errorDetails"] == self.report_error_message_2 )) or (
                self.pipeline_config['create_report_option']['data_by_asin_key'] in record_json.keys()
                and record_json[self.pipeline_config['create_report_option']['data_by_asin_key']] == []
            ):
                # if error is regarding the prohibited date given as a parameter to create report then return None
                print("Checkpoint none")
                return None

            if "errorDetails" in record_json.keys():
                raise Exception(record_json["errorDetails"])

            record_df = pd.json_normalize(record_json[self.pipeline_config['create_report_option']['data_by_asin_key']])

            if self.table_name == "sp_promotion_performance":
                jdf = record_df.explode('includedProducts').reset_index(drop = True)
                record_df = pd.concat([jdf, jdf["includedProducts"].apply(pd.Series)], axis=1).drop(['includedProducts'],axis=1)

            if self.table_name == "sp_coupon_performance":
                df = record_df.explode('coupons').reset_index(drop = True)
                jdf = pd.concat([df, df["coupons"].apply(pd.Series)], axis=1).drop(['coupons'],axis=1)
                df2 = jdf.explode('asins').reset_index(drop = True)
                record_df = pd.concat([df2, df2["asins"].apply(pd.Series)], axis=1).drop(['asins'],axis=1)

        raw_df = self.session.createDataFrame(record_df)
        del record_df
        raw_df = self.raw_dataframe_cleaning(raw_df, date_range)
        raw_df = raw_df.to_pandas()
        return raw_df


    def fetch_latest_available_report_end_date(
        self, aws_config, access_key, secret_key, session_token
    ):
        """
        Fetching latest end date for which given report can be fetch from sp api

        params
        aws_config: dict
        access_key: string
        secret_key: string
        session_token: string

        return
        end_date: string
        """
        print("Fetching latest_available_report_end_date")

        if self.table_name in self.large_json_tables:
            return (date.today() - timedelta(days=3)).strftime("%Y-%m-%d"), None
        
        end_date = date.today()
        if self.lookback_period_type == "DAY":
            lookback_days = 4 if self.table_name == "sp_inventory" else int(self.common_config['lookback_days_not_available_assumption']['DAY'])
            end_date = (end_date - timedelta(days=lookback_days)).strftime("%Y-%m-%d")
            while True:
                # Move a day back until the report for the given date is available
                start_date = end_date
                report_response = self.get_report_from_avc(
                    start_date, end_date, aws_config, access_key, secret_key, session_token
                )
                if (
                    "errorDetails" in report_response.keys()
                    and ( report_response["errorDetails"] == self.error_message_to_be_handle
                    or report_response["errorDetails"] == self.report_error_message_2 )
                ) or (
                    'reportRequestError' in report_response.keys() 
                    and self.market_basket_report_error_message in report_response['reportRequestError']
                ) or (
                    self.pipeline_config['create_report_option']['data_by_asin_key'] in report_response.keys()
                    and report_response[self.pipeline_config['create_report_option']['data_by_asin_key']] == []
                ):
                    end_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(1)).strftime('%Y-%m-%d')
                elif 'errorDetails' in report_response.keys():
                    raise Exception(report_response['errorDetails'])
                else:
                    break

        if self.lookback_period_type == "WEEK":
            end_date = end_date.strftime("%Y-%m-%d")
            if datetime.strptime(end_date, "%Y-%m-%d").weekday() != 5:
                # Fetching last saturday as end date if today is not Saturday
                end_date = date.today() - timedelta(((date.today().weekday() + 1) % 7) + 1)
                end_date = end_date.strftime("%Y-%m-%d")

            while True:
                # Move a week back until the report for the given week is available
                start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(6)).strftime("%Y-%m-%d")
                report_response = self.get_report_from_avc(
                    start_date, end_date, aws_config, access_key, secret_key, session_token
                )

                if (
                    "errorDetails" in report_response.keys()
                    and ( report_response["errorDetails"] == self.error_message_to_be_handle
                    or report_response["errorDetails"] == self.report_error_message_2 )
                ) or (
                    "reportRequestError" in report_response.keys()
                    and self.repeat_purchase_report_error_message in report_response["reportRequestError"]
                ):
                    end_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(7)).strftime("%Y-%m-%d")
                elif "errorDetails" in report_response.keys():
                    raise Exception(report_response["errorDetails"])
                else:
                    break
        print(f"Latest available end date for given report type : {end_date}")
        record_df = pd.json_normalize(report_response[self.pipeline_config['create_report_option']['data_by_asin_key']])
        raw_df = self.session.createDataFrame(record_df)
        del record_df
        raw_df = self.raw_dataframe_cleaning(raw_df, [start_date, end_date])
        raw_df = raw_df.to_pandas()
        return end_date, raw_df


    def fetch_required_parameter(self):
        """
        Fetch the variables required for the data ingestion

        return
        aws_config: dict
        access_key: string
        secret_key: string
        session_token: string
        start_date: string
        end_date: string
        """

        aws_config = self.aws_connect.get_aws_config_parameters()
        session, access_key, secret_key, session_token = self.aws_connect.get_aws_session_credentials(aws_config["region"])
        report_response = None

        if self.lookback_period is not None:
            lookback_period = int(self.lookback_period)

        if self.table_name == "sp_forecast":
            end_date = None
            start_date = None
        elif self.table_name in ["sp_promotion_performance", "sp_coupon_performance"]:
            end_date = datetime.now().strftime('%Y-%m-%dT%H:%M:%SZ')
            start_date = (datetime.now()-timedelta(days=int(lookback_period))).strftime('%Y-%m-%dT%H:%M:%SZ')
        else:
            end_date, report_response = self.fetch_latest_available_report_end_date(
                aws_config, access_key, secret_key, session_token
            )
            start_date = None

            if self.lookback_period_type == "DAY":
                if not self.backfilling_start_date:
                    start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta(lookback_period - 1)).strftime("%Y-%m-%d")
                else:
                    start_date = self.backfilling_start_date

            if self.lookback_period_type == "WEEK":
                start_date = (datetime.strptime(end_date, "%Y-%m-%d") - timedelta((lookback_period * 7) - 1)).strftime("%Y-%m-%d")
    
        return (aws_config, access_key, secret_key, session_token, start_date, end_date, report_response)


    def raw_dataframe_builder(self):
        """
        Build raw dataframe from the report we get from the amazon sp api
        """
        aws_config, access_key, secret_key, session_token, start_date, end_date, report_response = self.fetch_required_parameter()

        if self.table_name == "sp_forecast":
            df = self.generate_raw_dataframe(
                (start_date, end_date), aws_config, access_key, secret_key, session_token
            )
            lookback_date_list = [df['FORECASTGENERATIONDATE'].max().strftime("%Y-%m-%d")]
            already_in_s3_date_list = self.aws_connect.get_all_available_dates_for_one_table_in_s3(self.source, self.table_name)
            date_list = sorted(list(set(lookback_date_list).difference(already_in_s3_date_list)))
            if len(date_list) > 0:
                self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, lookback_date_list*2, self.source, self.table_name)

        elif self.table_name in ["sp_promotion_performance", "sp_coupon_performance"]:
            df = self.generate_raw_dataframe(
                (start_date, end_date), aws_config, access_key, secret_key, session_token
            )
            if df is not None:
                self.aws_connect.move_previous_csv_to_processed_folder([end_date], self.source, self.table_name)
                self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, [start_date.split("T")[0], end_date.split("T")[0]], self.source, self.table_name)

        else:
            # tables: sales, traffic, net_ppm, inventory, market_basket, repeat_purchase, inventory_weekly, search_terms
            lookback_date_list = self.generate_date_list_to_extract(end_date)
            already_in_s3_date_list = self.aws_connect.get_all_available_dates_for_one_table_in_s3(self.source, self.table_name)
            date_list = sorted(list(set(lookback_date_list).difference(already_in_s3_date_list)))
            date_list = self.aws_connect.adequate_date_list_for_requests(date_list)

            for date_range in date_list:
                if date_range[1] == end_date and report_response is not None:
                    df = report_response
                else: 
                    df = self.generate_raw_dataframe(
                        date_range, aws_config, access_key, secret_key, session_token
                    )
                if df is not None:
                    if self.pipeline_config['csv_column_names'] != sorted(list(df.columns)):
                        raise Exception(f"The column names check for {self.table_name}_{date_range[1]} has failed.")
                    self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, date_range, self.source, self.table_name)
                    del df
           

    def launch(self):
        self.raw_dataframe_builder()
  

def entrypoint(): 
    task = AVCIngestionTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
