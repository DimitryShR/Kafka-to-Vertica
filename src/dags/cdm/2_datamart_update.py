from airflow.decorators import dag, task

from cdm.vertica_connect import VertConnect
from cdm.config import ConfigApp
# from datetime import datetime, timedelta
import pendulum
from cdm.cdm_repository import CDM_Loader

import logging
# Настройка логгера
logging.basicConfig(level=logging.INFO) 
logger = logging.getLogger(__name__)

business_dt = '{{ ds }}'
op_kwargs={'load_dt': business_dt}

@dag(
    schedule_interval='0 10 * * *',  # Задаем расписание выполнения дага - каждый день в 10.00
    # start_date=(datetime.today() - timedelta(days=30)),  # Дата начала выполнения дага.
    # end_date=(datetime.today() - timedelta(days=1)),  # Дата окончания выполнения дага.
    start_date = pendulum.datetime(2022, 10, 1, tz="UTC"), # Дата начала выполнения дага.
    end_date = pendulum.datetime(2022, 11, 1, tz="UTC"), # Дата окончания выполнения дага.
    catchup=True,  # Нужно ли запускать даг за предыдущие периоды (с start_date до сегодня) - False (не нужно).
    tags=['final_sprint', 'to_cdm', 'global_metrics'],  # Теги, используются для фильтрации в интерфейсе Airflow.
    is_paused_upon_creation=True  # Остановлен/запущен при появлении. Сразу запущен.
)


def to_cdm_loader():
    # Инициализируем переменные
    ConfigApp.set()
    config = ConfigApp()

    # Инициализируем класс подключения к Vertica
    vert_connect = VertConnect(
            host=config.vertica_host,
            port=config.vertica_port,
            user=config.vertica_user,
            pw=config.vertica_password
    )

    @task(task_id="cdm_increment_load")

    def cdm_increment_load(op_kwargs):
        load_dt = op_kwargs["load_dt"]
        logger.info("load_dt is {ds}".format(ds=load_dt))

        # Инициализируем класс загрузчика витрины
        loader = CDM_Loader(vertica=vert_connect, logger=logger)
        # Запускаем инкрементальную загрузку
        loader.cdm_data_increment_load(load_dt)
 
    cdm_increment_loader = cdm_increment_load(op_kwargs)

    # Формируем порядок запуска тасков (пока таск 1)
    cdm_increment_loader

to_cdm_load_dag = to_cdm_loader()