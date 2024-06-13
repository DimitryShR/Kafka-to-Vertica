from to_stg.pg_connect import PgConnect
from typing import List, Optional, Union
from datetime import datetime
from pydantic import BaseModel
from psycopg.rows import class_row

from logging import Logger

class TransactionJsonObj(BaseModel):
    id: int
    msg_value: str
    msg_dttm: datetime

class TransactionRawRepository:
    def __init__(self, pg: PgConnect, logger: Logger) -> None:
        self._db = pg
        self.logger = logger

    def load_raw_transaction(self, object_type:str, last_loaded_id: int, load_date: datetime) -> Optional[List[TransactionJsonObj]]:
        with self._db.client().cursor(row_factory=class_row(TransactionJsonObj)) as cur:
            cur.execute(
                """
                    SELECT
                    id,
                    msg_value,
                    msg_dttm
                    FROM stg.transactions
                    WHERE 
                    object_type = %(object_type)s
                    AND id >= %(last_loaded_id)s
                    AND msg_dttm::date = %(load_date)s
                    ORDER BY id;
                """,
                {"object_type":object_type,
                 "last_loaded_id":last_loaded_id,
                 "load_date":load_date}
            )
            objs = cur.fetchall()

        if not objs:
            self.logger.info(f"No transaction data was received")
            return None

        self.logger.info(f"raw data with type = {object_type} and date = {load_date} loaded"), 
        return objs

