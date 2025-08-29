from utils.common import Task
from datetime import datetime
from utils.aws_connection import AWSConnection
from utils.snowpark_utils import snowflake_connection
import pandas as pd



class SellinTask(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source = "sellin"
        self.aws_connect=AWSConnection()
        self.session = snowflake_connection().sf_connection(prod_env=True)

    
    def get_sellin_dataset_from_sf(self):
        sql_query = self.pipeline_config[self.country_code]["sql_query"]
        df = self.session.sql(sql_query)
        df = df.to_pandas()
        return df
        #return list_dir
        
        
    def transform_df(self, df):
        df.columns = [col.upper() for col in df.columns]

        # Use CN instead of ZZ in MARKET Column if the country_code is CN
        if self.country_code == "CN" and "MARKET" in df.columns:
            df["MARKET"] = df["MARKET"].replace("ZZ", "CN")
            
        # Clean GMID with only 6 digits
        if self.country_code in ["US", "GB", "CN", "ES", "DE"] and "GMID" in df.columns:
            df["GMID"] = df["GMID"].astype(str).str.lstrip("0").str[-6:]
            
        return df
        
    def launch(self):
        df = self.get_sellin_dataset_from_sf()
        df = self.transform_df(df)

        extraction_date = datetime.today().strftime("%Y-%m-%d")
        self.aws_connect.move_previous_csv_to_processed_folder([], self.source, self.table_name)
        self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, [extraction_date, extraction_date], self.source, self.table_name, sort_cols=False)


def entrypoint():
    task = SellinTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
