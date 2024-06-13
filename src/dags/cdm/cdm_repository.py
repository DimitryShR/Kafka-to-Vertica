from cdm.vertica_connect import VertConnect
from datetime import date
from logging import Logger
import os

class CDM_Loader:
    def __init__(self, vertica: VertConnect, logger:Logger) -> None:
        self.vertica = vertica
        self.logger = logger

    def cdm_data_increment_load(self, dt_load:date):
        
        params = {
            "dt_load":str(dt_load)
        }

        global_metrics_insert_path = os.path.join(os.path.dirname(__file__), 'sql/Global_metrics_insert.sql')
        self.logger.info(f"Defined Insert.sql file path is {global_metrics_insert_path}")
        
        vertica_conn = self.vertica.connect()
        self.logger.info(f"The Vertica connection is established")
        
        with open(global_metrics_insert_path, 'r') as global_metrics_insert:
            with vertica_conn.cursor() as cur:
                cur.execute(global_metrics_insert.read(), params)

                vertica_conn.commit()
                
                self.logger.info(f"Increment for {dt_load} has been loaded")
            cur.close()