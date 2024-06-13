import os
from dotenv import load_dotenv

class ConfigApp():
    def __init__(self) -> None:
        self.kafka_host = str(os.getenv('KAFKA_HOST'))
        self.kafka_port = int(str(os.getenv('KAFKA_PORT')))
        self.kafka_consumer_user = str(os.getenv('KAFKA_CONSUMER_USERNAME'))
        self.kafka_consumer_password = str(os.getenv('KAFKA_CONSUMER_PASSWORD'))
        self.kafka_consumer_group = str(os.getenv('KAFKA_CONSUMER_GROUP'))
        self.kafka_consumer_topic = str(os.getenv('KAFKA_TRANSACTION_SERVICE_INPUT_TOPIC'))
        self.kafka_partition_count = int(os.getenv('KAFKA_PARTITIONS_COUNT'))


        self.pg_warehouse_host = str(os.getenv('PG_WAREHOUSE_HOST'))
        self.pg_warehouse_port = int(str(os.getenv('PG_WAREHOUSE_PORT')))
        self.pg_warehouse_dbname = str(os.getenv('PG_WAREHOUSE_DBNAME'))
        self.pg_warehouse_user = str(os.getenv('PG_WAREHOUSE_USER'))
        self.pg_warehouse_password = str(os.getenv('PG_WAREHOUSE_PASSWORD'))
        self.pg_warehouse_table = str(os.getenv('PG_WAREHOUSE_TRANSACTION_TABLE'))

        self.spark_postgres_package = str(os.getenv('SPARK_POSTGRES_PACKAGE'))
        self.spark_kafka_package = str(os.getenv('SPARK_KAFKA_PACKAGE'))
        
    def set() -> None:
        dotenv_path = os.path.join(os.path.dirname(__file__), '.env')
        if os.path.exists(dotenv_path):
            load_dotenv(dotenv_path)

# class EnvNm(ConfigConst):
#         KAFKA_HOST="KAFKA_HOST"
#         KAFKA_PORT="KAFKA_PORT"
#         KAFKA_CONSUMER_USERNAME="KAFKA_CONSUMER_USERNAME"
#         KAFKA_CONSUMER_PASSWORD="KAFKA_CONSUMER_PASSWORD"
#         KAFKA_CONSUMER_GROUP="KAFKA_CONSUMER_GROUP"
#         KAFKA_TRANSACTION_SERVICE_INPUT_TOPIC="KAFKA_TRANSACTION_SERVICE_INPUT_TOPIC"
#         KAFKA_PARTITIONS_COUNT="KAFKA_PARTITIONS_COUNT"
#         PG_WAREHOUSE_HOST="PG_WAREHOUSE_HOST"
#         PG_WAREHOUSE_PORT="PG_WAREHOUSE_PORT"
#         PG_WAREHOUSE_DBNAME="PG_WAREHOUSE_DBNAME"
#         PG_WAREHOUSE_TRANSACTION_TABLE="PG_WAREHOUSE_TRANSACTION_TABLE"
#         PG_WAREHOUSE_USER="PG_WAREHOUSE_USER"
#         PG_WAREHOUSE_PASSWORD="PG_WAREHOUSE_PASSWORD"
