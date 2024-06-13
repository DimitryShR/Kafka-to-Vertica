from to_stg.pg_connect import PgConnect
from to_stg.vertica_connect import VertConnect

from to_stg.setting_repository import EtlSettingsRepository, EtlSetting
from to_stg.transaction_raw_repository import TransactionRawRepository
from to_stg.transaction_stg_repository import TransactionStgRepository
from datetime import date
from logging import Logger


class TransactionLoader:
    LAST_LOADED_ID_KEY = "last_loaded_id"
    WF_KEY = 'transaction_raw_to_stg_loader'

    def __init__(self, pg: PgConnect, vertica: VertConnect, logger_:Logger, load_date:date, object_type:str, schema: str, table: str) -> None:
        self.raw = TransactionRawRepository(pg=pg, logger=logger_)
        self.stg = TransactionStgRepository(vertica=vertica, pg=pg, logger=logger_)
        self.setting = EtlSettingsRepository(pg=pg, logger=logger_)
        self.logger = logger_
        self.load_date = load_date
        self.object_type = object_type
        self.schema = schema
        self.table = table

    def load_transactions(self):
        
        etl_setting = self.setting.get_setting(self.WF_KEY)
        if not etl_setting:
            etl_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = int(etl_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.logger.info(f"For {self.WF_KEY} last_loaded_id = {last_loaded_id}")

        raw_data = self.raw.load_raw_transaction(object_type=self.object_type, last_loaded_id=last_loaded_id, load_date=self.load_date)
        parse_transaction = self.stg.parse_transaction(raw_data)
        self.stg.insert(parse_transaction, self.schema, self.table, self.WF_KEY, self.LAST_LOADED_ID_KEY)

class CurrencyLoader:
    LAST_LOADED_ID_KEY = "last_loaded_id"
    WF_KEY = 'currency_raw_to_stg_loader'

    def __init__(self, pg: PgConnect, vertica: VertConnect, logger_:Logger, load_date:date, object_type:str, schema: str, table: str) -> None:
        self.raw = TransactionRawRepository(pg, logger_)
        self.stg = TransactionStgRepository(vertica, pg, logger_)
        self.setting = EtlSettingsRepository(pg, logger_)
        self.logger = logger_
        self.load_date = load_date
        self.object_type = object_type
        self.schema = schema
        self.table = table

    def load_currency(self):
        
        etl_setting = self.setting.get_setting(self.WF_KEY)
        if not etl_setting:
            etl_setting = EtlSetting(self.WF_KEY, {self.LAST_LOADED_ID_KEY: -1})

        last_loaded_id = int(etl_setting.workflow_settings[self.LAST_LOADED_ID_KEY])

        self.logger.info(f"For {self.WF_KEY} last_loaded_id = {last_loaded_id}")

        raw_data = self.raw.load_raw_transaction(object_type=self.object_type, last_loaded_id=last_loaded_id, load_date=self.load_date)

        if raw_data:
            parse_currency = self.stg.parse_currency(raw_data)
            self.stg.insert(parse_currency, self.schema, self.table, self.WF_KEY, self.LAST_LOADED_ID_KEY)



        