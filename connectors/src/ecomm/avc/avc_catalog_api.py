
from datetime import datetime
from utils.common import Task
import pandas as pd
from snowflake.snowpark.functions import current_timestamp, col, lit
from utils.snowpark_utils import snowflake_connection, read_and_write
from utils.aws_connection import AWSConnection


class CatalogIngestion(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.catalog_config = self.pipeline_config
        self.common_config = self.common_conf
        self.access_token_endpoint = self.common_config["access_token_endpoint"]
        self.report_service = self.common_config["report_service"]
        self.table_list = self.common_config["table_list"]
        self.catalog_api_endpoint = self.common_config["catalog_api_endpoint"]
        self.aws_connect = AWSConnection()
        self.session = snowflake_connection().sf_connection()
        self.source = "avc"
        self.rw = read_and_write(self.session)
        self.country_code = "GB" if self.country_code == "UK" else self.country_code
    

    def list_of_dicts(self, ld):
        return dict([(k, v) for k, v in ld[0].items()])


    def create_dataframe_from_json_response(self, catalog_df, catalog_response):
        """
        Create dataframe from json response and do union with previous output

        params
            catalog_df: dataframe
            catalog_response: dict

        return
            catalog_df: dataframe
        """
        pd_df = pd.json_normalize(catalog_response['items'])
        summaries_df = pd.json_normalize(pd_df['summaries'].apply(self.list_of_dicts).tolist()).add_prefix('summaries.')
        vendorDetails_df = pd.json_normalize(pd_df['vendorDetails'].apply(self.list_of_dicts).tolist()).add_prefix('vendorDetails.')
        pd_df = pd_df[['asin']].join([summaries_df, vendorDetails_df])

        snow_df = self.session.createDataFrame(pd_df)

        if catalog_df is None:
            # If catalog dataframe is empty then assign snowpark dataframe created from first 20 asin
            catalog_df = snow_df
        else:
            # Create a unique list of column from two dataframe and create the missing column in the required dataframe
            catalog_columns = catalog_df.columns
            snow_df_columns = snow_df.columns
            unique_columns = set(catalog_columns + snow_df_columns)
            for column in unique_columns:
                # Iterate to create the missing column in required dataframe
                if column not in catalog_columns:
                    catalog_df = catalog_df.withColumn(column, lit(None))
                if column not in snow_df_columns:
                    snow_df = snow_df.withColumn(column, lit(None))
            catalog_df = catalog_df.unionByName(snow_df)

        return catalog_df


    def generate_catalog_dataframe(self, asin_list, aws_config, access_token, access_key, secret_key, session_token):
        """
        Create catalog dataframe from the asin_list by making request to catalog_endpoint and passing batch of 20 asins in every call

        params
            asin_list: list
            aws_config: dict
            access_token: string
            access_key: string
            secret_key: string
            session_token: string

        return
            catalog_df: dataframe
        """
        catalog_df = None
        while asin_list:
            # Iterate over the asin list and create a batch of 20 asins to append it in catalog endpoint path in every api request
            if len(asin_list) > 20:
                selected_asin_list = asin_list[:20]
                asin_list = asin_list[20:]
                identifier_list = "%2C".join(selected_asin_list)
            else:
                selected_asin_list = asin_list
                identifier_list = "%2C".join(selected_asin_list)
                asin_list = []

            catalog_endpoint = self.catalog_api_endpoint.format(
                identifier_list=identifier_list, base_url=aws_config['base_url'], marketplaceIds=aws_config['marketplaceIds'][0])

            catalog_response = self.aws_connect.api_call_with_delay('GET', aws_config['report_service'], aws_config['region'], access_token, catalog_endpoint,
                                                   None, access_key, secret_key, session_token)

            if catalog_response['numberOfResults'] == 0:
                # If there is no information for given list of asin then continue
                continue

            catalog_df = self.create_dataframe_from_json_response(catalog_df, catalog_response)
        return catalog_df


    def clean_dataframe_column_names(self, catalog_df):
        """
        Cleaning column name in catalog dataframe by removing special character and changing pandas column names to appropriate column name

        params
            catalog_df: dataframe

        return
            catalog_df: dataframe
        """
        print("Cleaning dataframe column names")
        catalog_df_columns = catalog_df.columns
        for column in catalog_df_columns:
            # Convert column name to uppercase,remove the prefix summaries and vendorDetails from column name and replace (.) with (_)
            catalog_df = catalog_df.withColumnRenamed(column, column.upper())
            if 'summaries' in column:
                rename_column = column.split('summaries.')[1].replace('"', '').replace('.', '_')
                if rename_column.upper() in catalog_df.columns:
                    catalog_df = catalog_df.drop(rename_column.upper())
                catalog_df = catalog_df.withColumnRenamed(column.upper(), rename_column.upper())
            elif 'vendorDetails' in column:
                rename_column = column.split('vendorDetails.')[1].replace('"', '').replace('.', '_')
                if rename_column.upper() in catalog_df.columns:
                    catalog_df = catalog_df.drop(rename_column.upper())
                catalog_df = catalog_df.withColumnRenamed(column.upper(), rename_column.upper())
        return catalog_df


    def clean_catalog_dataframe(self, catalog_df):
        """
        Cast date column to date data type and add last_updated_timestamp column

        params
            catalog_df: dataframe

        return
            catalog_df: dataframe
        """
        for column in catalog_df.columns:
            if 'date' in column.lower():
                catalog_df = catalog_df.withColumn(column, col(column).cast('date'))
        catalog_df = catalog_df.withColumn('COUNTRY_CODE', lit(self.country_code.upper()))
        catalog_df = catalog_df.withColumn('last_updated_timestamp', current_timestamp())
        return catalog_df


    def prepare_dataframe_with_unique_asin(self):
        """
        Create a dataframe with unique asin by collecting all asins from given raw table list

        return
            asin_df: dataframe
        """
        print("preparing dataframe_with_unique_asin")
        union_df = None
        asin_table_list = self.table_list
        for table_name in asin_table_list:
            # read all required tables to fetch unique asins
            raw_table_details = dict()
            raw_table_details['sfSchema'] = self.catalog_config['asin_tables']['sfSchema']
            raw_table_details['dbtable'] = table_name.strip()
            df = self.rw.read_from_sf(raw_table_details)
            df = df.filter(df["COUNTRY_CODE"]== self.country_code.upper()).select('asin')
            if union_df is None:
                union_df = df
            else:
                union_df = union_df.unionByName(df)
        asin_df = union_df.distinct()
        return asin_df
   
   
    def build_catalog_dataframe(self): 
        """
        Build catalog dataframe and write to snowflake
        """
        aws_config = self.aws_connect.get_aws_config_parameters()
        session, access_key, secret_key, session_token = self.aws_connect.get_aws_session_credentials(aws_config["region"])
        access_token = self.aws_connect.get_access_token(self.access_token_endpoint, aws_config['access_token_request_body'])
        # Prepare dataframe with unique asin and Convert dataframe to list of asins
        asin_df = self.prepare_dataframe_with_unique_asin()
        asin_row_values = asin_df.collect()
        asin_list = [x.asDict()['ASIN'] for x in asin_row_values]       

        df = self.generate_catalog_dataframe(asin_list, aws_config, access_token, access_key, secret_key, session_token)
        raw_catalog = self.clean_dataframe_column_names(df)
        catalog_df = self.clean_catalog_dataframe(raw_catalog)  
        df = catalog_df.to_pandas()

        extraction_date = datetime.today().strftime("%Y-%m-%d")
        self.aws_connect.move_previous_csv_to_processed_folder([], self.source, self.table_name)
        self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, [extraction_date, extraction_date], self.source, self.table_name)
   

    def launch(self):
        self.build_catalog_dataframe()


def entrypoint():
    task = CatalogIngestion()
    task.launch()


if __name__ == "__main__":
    entrypoint()
