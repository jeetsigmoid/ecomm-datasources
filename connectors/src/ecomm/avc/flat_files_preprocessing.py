
from utils.common import Task
import pandas as pd
import numpy as np
from utils.aws_connection import AWSConnection


class FileIngestionTask(Task):
    def __init__(self, init_conf=None):
        super().__init__(init_conf)
        self.source = "avc"
        self.aws_connect=AWSConnection()
        self.file_date = ""
        self.file_name = ""


    def get_raw_file_from_s3(self):
        bytes_obj, file_date, file_name = self.aws_connect.get_most_recent_flat_file_in_bytes()
        self.file_date = file_date
        self.file_name = file_name
        print(file_date)
        print(file_name) 
        try:
            df = pd.read_excel(bytes_obj)
        except:
            try:
                df = pd.read_csv(bytes_obj)
            except:
                raise Exception("The flat file must be xlsx or csv format.")
        return df, file_date, file_name
    

    def transform_file(self, df):
        if self.table_name == "hero_mapping":
            if "CATEGORY" not in df.columns:
                df["CATEGORY"] = str(np.nan)
            df = df[self.pipeline_config['csv_column_names']]
            df['SHIPPED_COGS'] = df['SHIPPED_COGS'].apply(lambda x: '' if x == "-" else x)
            df = df.replace({"BRAND": self.pipeline_config['jp_brands_translation']})
        return df
    

    def preprocess_file(self):
        df,file_date,file_name = self.get_raw_file_from_s3()
        df = self.transform_file(df)
        if isinstance(file_name, list):
            file_name = ", ".join(file_name)

        df["FILENAME"] = file_name
        self.aws_connect.move_previous_csv_to_processed_folder([], self.source, self.table_name)
        self.aws_connect.copy_dataframe_as_csv_into_s3_bucket(df, [self.file_date]*2, self.source, self.table_name)


    def launch(self):
        self.preprocess_file()
  

def entrypoint(): 
    task = FileIngestionTask()
    task.launch()


if __name__ == "__main__":
    entrypoint()      
