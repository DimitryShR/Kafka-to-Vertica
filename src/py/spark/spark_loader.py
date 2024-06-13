from spark_connect import SparkSessionBuilder, KafkaConnect, PgConnect
from config import ConfigApp

# import findspark
# findspark.init()
# findspark.find() 

# from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType,TimestampType, IntegerType

import logging
# Настройка логгера
logging.basicConfig(level=logging.ERROR) 
logger = logging.getLogger(__name__)

def read_jdbc(spark, options):
    try:
        read_df = spark.read \
                .format('jdbc') \
                .options(**options) \
                .load()
        
        logger.info(f'Data from jdbs is read')

        return read_df
    except:
        logger.error(f'Error reading jdbc data with next options {options}')

def get_offset(df, topic:str, partition_count:int):
    
    partition_offset_dict = {}
    for partition in range(partition_count):
        partition_offset_dict[str(partition)] = 0

    try:
        offset_list = df \
            .select("partition", "offset") \
            .groupBy("partition") \
            .agg(f.max("offset").alias("max_offset")) \
            .collect()
        logger.info(f'The list of offsets is received')

        for each in offset_list:
            partition, offset = each
            partition_offset_dict[str(partition)] = offset + 1
        
        logger.info(f'The list of offsets has been updated')
        return str({topic:partition_offset_dict}).replace('\'', '\"')
    
    except:
        logger.error(f'The list of offsets has not been received')
        return str({topic:partition_offset_dict}).replace('\'', '\"')
        
def read_stream(spark, options, offset, batch_size):
    try:
        read_stream_df = spark.readStream \
        .format('kafka') \
        .options(**options) \
        .option("startingOffsets", offset) \
        .option("maxOffsetsPerTrigger", batch_size) \
        .load()

        logger.info(f'Started reading kafka stream with next options {options} and offset {offset}')
        return read_stream_df
    except:
        logger.error(f'Error reading kafka stream with next options {options} and offset {offset}')

def transform_stream(df):
    # # определяем схему входного сообщения для json
    incomming_message_schema  = StructType([
                        StructField("object_id", StringType(), True), \
                        StructField("object_type", StringType(), True)
                        ])
                        # StructField("sent_dttm", TimestampType(), True), \
                        # StructField("payload", StringType(), True)
                        

    return df \
            .withColumn('parse_value', f.from_json(f.col("value").cast(StringType()), incomming_message_schema)) \
            .selectExpr('parse_value.*', 'value', 'topic', 'partition', 'offset', 'timestamp') \
            .withColumn('msg_value', f.col('value').cast(StringType())) \
            .withColumn('topic', f.col('topic').cast(StringType())) \
            .withColumn('partition', f.col('partition').cast(IntegerType())) \
            .withColumn('offset', f.col('offset').cast(IntegerType())) \
            .withColumn('msg_dttm', f.col('timestamp').cast(TimestampType())) \
            .select("object_id", "object_type", "msg_value", "topic", "partition", "offset", "msg_dttm")
    # return df \
    #         .withColumn('msg_value', f.col('value').cast(StringType())) \
    #         .withColumn('topic', f.col('topic').cast(StringType())) \
    #         .withColumn('partition', f.col('partition').cast(IntegerType())) \
    #         .withColumn('offset', f.col('offset').cast(IntegerType())) \
    #         .withColumn('msg_dttm', f.col('timestamp').cast(TimestampType())) \
    #         .select("msg_value", "topic", "partition", "offset", "msg_dttm")

def save_to_postgresql(df, epoch_id):
    df.write \
    .mode("append") \
    .format("jdbc") \
    .options(**postgres_writing_options) \
    .save()

def main():

    config = ConfigApp()
        
    spark = SparkSessionBuilder(appname="TransactionInputService",
                                packages=[config.spark_postgres_package,
                                          config.spark_kafka_package]
                                ).create_session()
    
    pg_options = PgConnect(
            host=config.pg_warehouse_host,
            port=config.pg_warehouse_port,
            dbname=config.pg_warehouse_dbname,
            user=config.pg_warehouse_user,
            pw=config.pg_warehouse_password,
            dbtable=config.pg_warehouse_table
            ).get_read_options()
    
    offset = get_offset(read_jdbc(spark, options=pg_options), 
                                     topic=config.kafka_consumer_topic,
                                     partition_count=config.kafka_partition_count)

    kafka_options = KafkaConnect(
        host=config.kafka_host,
        port=config.kafka_port,
        user=config.kafka_consumer_user,
        pw=config.kafka_consumer_password,
        topic=config.kafka_consumer_topic
        ).get_options()
    
    read_stream_df = read_stream(spark, options=kafka_options, offset=offset, batch_size=1000)
    
    transformed_stream = transform_stream(read_stream_df)

    transformed_stream.writeStream \
        .foreachBatch(save_to_postgresql) \
        .start() \
        .awaitTermination()
    
    spark.stop() 

    # transformed_stream.writeStream \
    #     .format("console") \
    #     .option("truncate", "false") \
    #     .trigger(continuous='5 seconds') \
    #     .start() \
    #     .awaitTermination() 

if __name__ == '__main__':
    
    
    ConfigApp.set()
    config = ConfigApp()

    postgres_writing_options = PgConnect(
            host=config.pg_warehouse_host,
            port=config.pg_warehouse_port,
            dbname=config.pg_warehouse_dbname,
            user=config.pg_warehouse_user,
            pw=config.pg_warehouse_password,
            dbtable=config.pg_warehouse_table
            ).get_write_options()

    main()