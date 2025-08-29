from abc import ABC, abstractmethod
from argparse import ArgumentParser
from datetime import datetime, timedelta
from typing import Dict, Any
import yaml
import pathlib
import sys
import os
import boto3


if os.getcwd().startswith('/home/u10'):
    from dotenv import load_dotenv
    load_dotenv(dotenv_path='../../.env', override=True)


class Task(ABC):
    """
    This is an abstract class that provides handy interfaces to implement workloads (e.g. jobs or job tasks).
    Create a child from this class and implement the abstract launch method.
    This class handles the loading of the .yaml configuration files and
    and the secrets across different environments 
    """

    def __init__(self, init_conf=None, **kwargs):
        self.args = self._get_conf()
        print(self.args)
        if init_conf:
            self.pipeline_config = init_conf
        else:
            self.pipeline_config = self._provide_config(self.args.conf_file)
        self.common_conf = self._provide_config(self.args.common_conf)
        self.country_code = self.args.country_code
        self.table_name = self.args.conf_file.split("/")[-1].split(".")[0]
        self.lookback_period_type = self.args.lookback_period_type
        self.lookback_period = self.args.lookback_period
        self.backfilling_start_date = self.args.backfilling_start_date
        # self.env = 'DEV'
        self.env = self._get_env_var("MWAA_ENV").upper()


    def _provide_config(self, conf_file):
        if not conf_file:
            return {}
        else:
            return self._read_config(conf_file)


    @staticmethod
    def _get_conf():
        p = ArgumentParser()
        p.add_argument("--conf_file", type=str, required=False)
        p.add_argument("--common_conf", type=str, required=False)
        p.add_argument("--country_code", type=str, required=False)
        p.add_argument("--lookback_period_type", type=str, required=False)
        p.add_argument("--lookback_period", type=str, required=False)
        p.add_argument("--backfilling_start_date", type=str, required=False)
        namespace = p.parse_known_args(sys.argv[1:])[0]
        return namespace
 

    def _read_config(self, conf_file) -> Dict[str, Any]:
        config = yaml.safe_load(pathlib.Path(conf_file).read_text())
        return config
    

    def _get_env_var(self, var_name):
        if os.getcwd().startswith('/home/u10'):
            return os.environ.get(var_name)
        else:
            secret_manager = boto3.client("secretsmanager", region_name="eu-west-1")
            return secret_manager.get_secret_value(
                SecretId=f"PROJECT/chc-ecommerce-analytics/{var_name}")["SecretString"]
        

    def generate_date_list_to_extract(self, last_available_date):
        """
        Based on the last available date in the avc endpoint and on the lookback_period
        parameter, it generates the list of dates to later check if they are already
        present in the S3 bucket

        params
        last_available_date: string

        return
        desired_dates: List[string]
        """
        end_date_dt = datetime.strptime(last_available_date, "%Y-%m-%d")
        if self.pipeline_config['create_report_option']['reportOptions']['reportPeriod'] == 'DAY':
            lookback_period = int(self.lookback_period) if not self.backfilling_start_date else (end_date_dt - datetime.strptime(self.backfilling_start_date, "%Y-%m-%d")).days
            desired_dates = [(end_date_dt - timedelta(days=i)).strftime("%Y-%m-%d") for i in range(int(lookback_period))]
        if self.pipeline_config['create_report_option']['reportOptions']['reportPeriod'] == 'WEEK':
            lookback_period = int(self.lookback_period) if not self.backfilling_start_date else int((end_date_dt - datetime.strptime(self.backfilling_start_date, "%Y-%m-%d")).days / 7)
            desired_dates = [(end_date_dt - timedelta(weeks=i)).strftime("%Y-%m-%d") for i in range(int(lookback_period))]
        return desired_dates        


    @abstractmethod
    def launch(self):
        pass
