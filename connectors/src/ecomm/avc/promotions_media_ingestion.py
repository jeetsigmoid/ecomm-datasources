from utils.common import Task
from datetime import datetime
from utils.aws_connection import AWSConnection
from utils.snowpark_utils import snowflake_connection
import pandas as pd
import numpy as np


class PromotionsTask(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source = "promotions_media"
        self.aws_connect=AWSConnection()
        self.session = snowflake_connection().sf_connection(prod_env=True)

    
    def get_promotions_dataset_from_sf(self):
        sql_query = self.pipeline_config[self.country_code]["sql_query"]
        df = self.session.sql(sql_query)
        df = df.to_pandas()
        return df
        #return list_dir
        
        
    def transform_df(self, df):
        if self.country_code == "CN":
            df.columns = [col.upper().strip() for col in df.columns]
        
            for col in ["CAMPAIGN_START_DATE", "CAMPAIGN_END_DATE"]:
                df[col] = pd.to_datetime(df[col].astype(str), format="%Y%m%d", errors='coerce').dt.strftime('%Y-%m-%d')
        
        return df
        
    def launch(self):
        if self.country_code == "US":
            df = self.get_promotions_dataset_from_sf()
            extraction_date = datetime.today().strftime("%Y-%m-%d")
        else:
            bytes_obj, extraction_date, file_name = self.aws_connect.get_most_recent_flat_file_in_bytes()
            df = pd.read_excel(bytes_obj)

        df = self.transform_df(df)
        print(df)
        
        self.aws_connect.move_previous_csv_to_processed_folder([], self.source, self.table_name)
        self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, [extraction_date, extraction_date], self.source, self.table_name, sort_cols=True if self.country_code not in ["US","CN"] else False)


def entrypoint():
    task = PromotionsTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()
