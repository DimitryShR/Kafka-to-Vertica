from airflow.decorators import dag, task

from to_stg.pg_connect import PgConnect
from to_stg.vertica_connect import VertConnect
from to_stg.config import ConfigApp
from to_stg.stg_loader import TransactionLoader, CurrencyLoader
from datetime import datetime, timedelta

import logging
# Настройка логгера
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)
business_dt = '{{ ds }}'
op_kwargs={'business_dt': business_dt}

@dag(
    schedule_interval='0 10 * * *',  # Задаем расписание выполнения дага - каждый день в 10.00
    start_date=(datetime.today() - timedelta(days=3)),  # Дата начала выполнения дага.
    end_date=(datetime.today() - timedelta(days=1)),  # Дата окончания выполнения дага.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['final_sprint', 'to_stg', 'transaction'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=False  # Остановлен/запущен при появлении. Сразу запущен.
)

def to_stg_transaction_loader():

    # Инициализируем переменные
    ConfigApp.set()
    config = ConfigApp()

    # Инициализируем класс подключения к Postgres
    pg_connect = PgConnect(
            host=config.pg_warehouse_host,
            port=config.pg_warehouse_port,
            dbname=config.pg_warehouse_dbname,
            user=config.pg_warehouse_user,
            pw=config.pg_warehouse_password
    )
    # Инициализируем класс подключения к Vertica
    vert_connect = VertConnect(
            host=config.vertica_host,
            port=config.vertica_port,
            user=config.vertica_user,
            pw=config.vertica_password
    )

    @task(task_id="transaction_load")
    def load_transactions(op_kwargs):
        OBJECT_TYPE = "TRANSACTION"
        SCHEMA = "STV2023121128__STAGING"
        TABLE = "transactions"

        load_dt = op_kwargs["business_dt"]
        logger.info("load_dt is {ds}".format(ds=load_dt))
        # Инициализируем класс, в котором реализована логика чтения, трансформации и загрузки данных
        loader = TransactionLoader(pg=pg_connect, vertica=vert_connect, logger_=logger, load_date=load_dt, object_type=OBJECT_TYPE, schema=SCHEMA, table=TABLE)

        # Запускаем копирование данных
        loader.load_transactions()

    @task(task_id="currency_load")
    def load_currency(op_kwargs):
        OBJECT_TYPE = "CURRENCY"
        SCHEMA = "STV2023121128__STAGING"
        TABLE = "currency"
        
        load_dt = op_kwargs["business_dt"]
        logger.info("load_dt is {ds}".format(ds=load_dt))
        # Инициализируем класс, в котором реализована логика чтения, трансформации и загрузки данных
        loader = CurrencyLoader(pg=pg_connect, vertica=vert_connect, logger_=logger, load_date=load_dt, object_type=OBJECT_TYPE, schema=SCHEMA, table=TABLE)

        # Запускаем копирование данных
        loader.load_currency()

    currency_loader = load_currency(op_kwargs)
    transactions_loder = load_transactions(op_kwargs)

    transactions_loder >> currency_loader

to_stg_transaction_dag = to_stg_transaction_loader()