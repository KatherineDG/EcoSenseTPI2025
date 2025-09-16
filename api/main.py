













# from dotenv import load_dotenv
# import os
# import findspark
# from pyspark.sql import SparkSession
# from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType

# load_dotenv()

# JAVA_HOME = os.getenv("JAVA_HOME")
# SPARK_HOME = os.getenv("SPARK_HOME")
# HADOOP_HOME = os.getenv("HADOOP_HOME")

# os.environ["JAVA_HOME"] = JAVA_HOME
# os.environ["SPARK_HOME"] = SPARK_HOME
# os.environ["HADOOP_HOME"] = HADOOP_HOME
# os.environ["PATH"] = os.path.join(JAVA_HOME, "bin") + os.pathsep + \
#                      os.path.join(SPARK_HOME, "bin") + os.pathsep + \
#                      os.path.join(HADOOP_HOME, "bin") + os.pathsep + \
#                      os.environ["PATH"]

# AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID")
# AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY")
# AWS_SESSION_TOKEN = os.getenv("AWS_SESSION_TOKEN")
# AWS_REGION = os.getenv("AWS_REGION")

# findspark.init(SPARK_HOME)

# jars = [
#     "file:///D:/Developer/spark/spark-3.5.2-bin-hadoop3/jars/hadoop-aws-3.3.4.jar",
#     "file:///D:/Developer/spark/spark-3.5.2-bin-hadoop3/jars/aws-java-sdk-bundle-1.12.510.jar"
# ]

# spark = SparkSession.builder \
#     .appName("SparkS3Example") \
#     .config("spark.master", "local[*]") \
#     .config("spark.jars", ",".join(jars)) \
#     .getOrCreate()

# hadoop_conf = spark._jsc.hadoopConfiguration()
# hadoop_conf.set("fs.s3a.access.key", AWS_ACCESS_KEY_ID)
# hadoop_conf.set("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY)
# hadoop_conf.set("fs.s3a.session.token", AWS_SESSION_TOKEN)
# hadoop_conf.set("fs.s3a.endpoint", f"s3.{AWS_REGION}.amazonaws.com")
# hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
# hadoop_conf.set("fs.s3a.connection.ssl.enabled", "true")
# hadoop_conf.set("fs.s3a.path.style.access", "true")

# schema = StructType([
#     StructField("sensorId", StringType(), True),
#     StructField("sensorType", StringType(), True),
#     StructField("amountDetected", DoubleType(), True),
#     StructField("unit", StringType(), True),
#     StructField("timestamp", StringType(), True),
#     StructField("sede", StringType(), True)
# ])

# df = spark.read.option("multiline", True).json("s3a://datalake-sensors-bucket/sensors-water/*.json")
# df.show(truncate=False)

# print("✅ Spark conectado a S3 correctamente")
