from snowflake.snowpark import Session
from utils.common import Task


class snowflake_connection(Task):
    def __init__(self, init_conf=None, **kwargs):
        super().__init__(init_conf)
        

    def sf_connection(self, prod_env=False):
        env = "PROD" if prod_env else self.env.upper()
        connection_parameters = self.get_sf_connection_parameters(env)
        session = Session.builder.configs(connection_parameters).create()
        return session


    def get_sf_connection_parameters(self, env):
            """read config from the yaml file and setup input for the connection
            Returns:
                connection_parameters: dict
            """
            connection_parameters = {
                    "account": "SANOFI-EMEA_DF_CHC", 
                    "user": self._get_env_var(f"MWAA_ECOMM_{env}_TRANSFORM_USER"),
                    "password": self._get_env_var(f"MWAA_ECOMM_{env}_TRANSFORM_PWD"),
                    "role": f"DF_CHC_ECOMMERCE_{env}_TRANSFORM_PROC",
                    "warehouse": f"DF_CHC_ECOMMERCE_{env}_WH_TRANSFORM",
                    "database": f"DF_CHC_ECOMMERCE_{env}",
                    "schema": "TECH"
                }
            
            return connection_parameters

    def launch(self):
       pass


class read_and_write(snowflake_connection):
    def __init__(self, session, init_conf=None):
        super().__init__(init_conf)
        self.session = session
        self.snowflake_db_name = session.get_current_database()


    def read_from_sf(self,
                     table_details):
        """
            Utility function to read table from snowflake using snowpark

            params
            table_details: dict     dictionary containing table information

            return
            df: dataframe
        """
        table_name = f"{self.snowflake_db_name}.{table_details['sfSchema']}.{table_details['dbtable']}"
        df = self.session.read.table(table_name)
        return df    
