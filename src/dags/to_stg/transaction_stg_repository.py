from to_stg.vertica_connect import VertConnect
from to_stg.pg_connect import PgConnect
from typing import List, Optional, Union
from datetime import datetime, date
from pydantic import BaseModel
import contextlib
import json
import pandas as pd

from logging import Logger

from to_stg.transaction_raw_repository import TransactionJsonObj
from to_stg.setting_repository import EtlSettingsRepository, EtlSetting

class TransactionStgObj(BaseModel):
    id: int
    object_id: str
    msg_dttm: datetime
    sent_dttm: datetime
    # payload: str
    operation_id: str
    account_number_from: int
    account_number_to: int
    currency_code: int
    country: str
    status: str
    transaction_type: str
    amount: float
    transaction_dt: datetime

class CurrencyStgObj(BaseModel):
    id: int
    object_id: str
    msg_dttm: datetime
    sent_dttm: datetime
    # payload: str
    date_update: datetime
    currency_code: int
    currency_code_with: int
    currency_with_div: float


class TransactionStgRepository:
    def __init__(self, vertica: VertConnect, pg: PgConnect, logger: Logger) -> None:
        self.vert = vertica
        self.setting = EtlSettingsRepository(pg=pg, logger=logger)
        self.logger = logger

    def parse_transaction(self, raws: List[TransactionJsonObj]) -> List[TransactionStgObj]:
        res = []
        # f_raws = filter(lambda a: a.object_type == "TRANSACTION", raws)
        for raw in raws:
            r_json = json.loads(raw.msg_value)
            r = TransactionStgObj(
                id = raw.id,
                object_id=r_json["object_id"],
                msg_dttm=raw.msg_dttm,
                sent_dttm=datetime.strptime(r_json["sent_dttm"], "%Y-%m-%dT%H:%M:%S"),
                operation_id=r_json["payload"]["operation_id"],
                account_number_from=r_json["payload"]["account_number_from"],
                account_number_to=r_json["payload"]["account_number_to"],
                currency_code=r_json["payload"]["currency_code"],
                country=r_json["payload"]["country"],
                status=r_json["payload"]["status"],
                transaction_type=r_json["payload"]["transaction_type"],
                amount=r_json["payload"]["amount"],
                transaction_dt=r_json["payload"]["transaction_dt"]                
                )
            res.append(r)

        self.logger.info("transaction has been parsed")
        return res
    
    def parse_currency(self, raws: List[TransactionJsonObj]) -> List[CurrencyStgObj]:
        res = []
        # f_raws = filter(lambda a: a.object_type == "CURRENCY", raws)
        for raw in raws:
            r_json = json.loads(raw.msg_value)
            r = CurrencyStgObj(
                id = raw.id,
                object_id=r_json["object_id"],
                msg_dttm=raw.msg_dttm,
                sent_dttm=r_json["sent_dttm"],
                date_update=r_json["payload"]["date_update"],
                currency_code=r_json["payload"]["currency_code"],
                currency_code_with=r_json["payload"]["currency_code_with"],
                currency_with_div=r_json["payload"]["currency_with_div"]
                )
            res.append(r)
        
        self.logger.info("currancy has been parsed")
        return res

    def transform_transaction(self, parse_list: Union[List[TransactionStgObj], List[CurrencyStgObj]]) -> pd.DataFrame:
        df = pd.DataFrame([parse_row.model_dump() for parse_row in parse_list])
        self.logger.info("Data transform to Pandas DataFrame")
        return df

    def insert_transaction(self, df: pd.DataFrame, schema, table, wf_key, last_loaded_id_key) -> None:
        # columns_list = list(df.columns)
        # columns_list.remove("id")
        # columns = ",".join(columns_list)
        columns = ",".join(df.columns)
        row_count = len(df)
        copy_query = f"""
        COPY {schema}.{table} ({columns}) FROM STDIN DELIMITER ',' ENCLOSED BY '"'
        REJECTED DATA AS TABLE {schema}.{table}_rej
        """
        vertica_conn = self.vert.connect()
        chunk_size = row_count // 10
        with contextlib.closing(vertica_conn.cursor()) as cur:
            start = 0
            while start <= row_count:
                end = min(start + chunk_size, row_count-1)
                self.logger.info(f"loading rows {start}-{end}")
                df.loc[start: end].to_csv('/tmp/chunk.csv', index=False) #, quoting = csv.QUOTE_NONNUMERIC ,escapechar="*"
                with open('/tmp/chunk.csv', 'rb') as chunk:
                    cur.copy(copy_query, chunk, buffer_size=65536)
                vertica_conn.commit()
                self.logger.info("loaded")

                last_loaded_id = int(df.loc[end].get('id'))
                etl_setting = EtlSetting(wf_key, {last_loaded_id_key: last_loaded_id})
                self.setting.save_setting(etl_setting)
                self.logger.info("etl_setting reloaded")

                start += chunk_size + 1
        vertica_conn.close()

    def insert(self, parse_list: Union[List[TransactionStgObj], List[CurrencyStgObj]], schema, table, wf_key, last_loaded_id_key):
        self.insert_transaction(self.transform_transaction(parse_list), schema, table, wf_key, last_loaded_id_key)

       


