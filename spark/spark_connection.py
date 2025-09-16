from dotenv import load_dotenv
import os
import findspark
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

load_dotenv()

JAVA_HOME = os.getenv("JAVA_HOME")
SPARK_HOME = os.getenv("SPARK_HOME")
HADOOP_HOME = os.getenv("HADOOP_HOME")

os.environ["JAVA_HOME"] = JAVA_HOME
os.environ["SPARK_HOME"] = SPARK_HOME
os.environ["HADOOP_HOME"] = HADOOP_HOME
os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + \
                     os.path.join(SPARK_HOME, "bin") + os.pathsep + \
                     os.path.join(HADOOP_HOME, "bin") + os.pathsep + \
                     os.environ["PATH"]

MONGODB_URI = os.getenv("MONGODB_URI")
NAME_DB = os.getenv("NAME_DB")

def create_spark_session():
    findspark.init(SPARK_HOME)

    spark = SparkSession.builder \
        .appName("MongoSpark") \
        .config("spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:10.3.0") \
        .config("spark.mongodb.read.connection.uri", MONGODB_URI) \
        .config("spark.mongodb.write.connection.uri", MONGODB_URI) \
        .getOrCreate()
    
    return spark


schema = StructType([
    StructField("_id", StringType(), True),
    StructField("company", StringType(), True),
    StructField("field", StringType(), True),
    StructField("sensor", StringType(), True),
    StructField("value", DoubleType(), True),
    StructField("timestamp", StringType(), True)
])

def read_mongo(spark, collection_name):
    return spark.read.format("mongodb") \
        .option("database", NAME_DB) \
        .option("collection", collection_name) \
        .schema(schema) \
        .load()

print("✅ Spark conectado a MONGODB correctamente")
