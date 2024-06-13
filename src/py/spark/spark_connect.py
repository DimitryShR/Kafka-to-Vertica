from pyspark.sql import SparkSession

class PgConnect:
    def __init__(self, host:str, port:int, dbname:str, user:str, pw:str, dbtable:str, driver:str="org.postgresql.Driver", commit_mode:str="true"):
        self.host = host
        self.port = port
        self.dbname = dbname
        self.user = user
        self.pw = pw
        self.dbtable = dbtable
        self.driver = driver
        self.commit_mode = commit_mode

    def get_read_options(self) -> dict:
        options = {
            "url":f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}",
            "driver":self.driver,
            "dbtable":self.dbtable,
            "user":self.user,
            "password":self.pw
        }
        return options
    
    def get_write_options(self) -> dict:
        options = {
            "url":f"jdbc:postgresql://{self.host}:{self.port}/{self.dbname}",
            "driver":self.driver,
            "dbtable":self.dbtable,
            "user":self.user,
            "password":self.pw,
            "autoCommit":self.commit_mode
        }
        return options

class KafkaConnect:
    def __init__(self, host:str, port:int, user:str, pw:str, topic:str, security_protocol:str="SASL_SSL", sasl_mechanism:str="SCRAM-SHA-512"):
        self.host = host
        self.port = port
        self.security_protocol = security_protocol
        self.sasl_mechanism = sasl_mechanism
        self.user = user
        self.pw = pw
        self.topic = topic

    def get_options(self) -> dict:
        options = {
            "kafka.bootstrap.servers": f"{self.host}:{self.port}",
            "kafka.security.protocol": self.security_protocol,
            "kafka.sasl.jaas.config":f"org.apache.kafka.common.security.scram.ScramLoginModule required username=\"{self.user}\" password=\"{self.pw}\";",
            "kafka.sasl.mechanism": self.sasl_mechanism,
            "subscribe": self.topic
        }
        return options
    
class SparkSessionBuilder:
    def __init__(self, appname:str, packages:list) -> None:
        self.packages = packages
        self.appname = appname

    def create_session(self):
        spark_jars_packages = ",".join(self.packages)
        spark = SparkSession.builder \
            .appName(self.appname) \
            .config("spark.sql.session.timeZone", "UTC") \
            .config("spark.jars.packages", spark_jars_packages) \
            .getOrCreate()
        return spark