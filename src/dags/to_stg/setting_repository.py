from typing import Dict, Optional

# from psycopg import Connection
from to_stg.pg_connect import PgConnect
from psycopg.rows import class_row
from pydantic import BaseModel
import json
from logging import Logger

class SettingRecord(BaseModel):
    workflow_key: str
    workflow_settings: Dict

class EtlSetting:
    def __init__(self, wf_key: str, setting: Dict) -> None:
        self.workflow_key = wf_key
        self.workflow_settings = setting
        

class EtlSettingsRepository:
    def __init__(self, pg: PgConnect, logger: Logger) -> None:
        self._db = pg
        self.logger = logger

    def get_setting(self, etl_key: str) -> Optional[EtlSetting]:
        with self._db.client() as conn:
            with conn.cursor(row_factory=class_row(SettingRecord)) as cur:
                cur.execute(
                    """
                        SELECT
                            workflow_key,
                            workflow_settings
                        FROM stg.wf_settings
                        WHERE workflow_key = %(etl_key)s;
                    """,
                    {"etl_key": etl_key},
                )
                obj = cur.fetchone()
        
        if not obj:
            self.logger.info(f"No workflow_settings was received")
            return None
        
        return EtlSetting(obj.workflow_key, obj.workflow_settings)
        # return EtlSetting(obj.workflow_key, json.loads(obj.workflow_settings))
    
    def save_setting(self, sett: EtlSetting) -> None:
        with self._db.client() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """
                        INSERT INTO stg.wf_settings(workflow_key, workflow_settings)
                        VALUES (%(etl_key)s, %(etl_setting)s)
                        ON CONFLICT (workflow_key) DO UPDATE
                        SET workflow_settings = EXCLUDED.workflow_settings;
                    """,
                    {
                        "etl_key": sett.workflow_key,
                        "etl_setting": json.dumps(sett.workflow_settings)
                    },
                )
                conn.commit()
                self.logger.info(f"workflow_settings has been saved")