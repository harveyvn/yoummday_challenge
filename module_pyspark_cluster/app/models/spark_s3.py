import os
from pyspark.sql import SparkSession


class SparkS3:
	def __init__(self, app_name: str, endpoint: str, access_key: str, secret_key: str):
		self._spark = self._init_spark(app_name, endpoint, access_key, secret_key)

	@staticmethod
	def _init_spark(app_name: str, endpoint: str, access_key: str, secret_key: str) -> SparkSession:
		master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
		spark_packages = ",".join(["org.apache.hadoop:hadoop-aws:3.3.1", "com.amazonaws:aws-java-sdk-bundle:1.11.901"])
		spark = SparkSession.builder.appName(app_name).master(master_url).config("spark.jars.packages", spark_packages).getOrCreate()
		hadoop_conf = spark._jsc.hadoopConfiguration()
		hadoop_conf.set("fs.s3a.endpoint", endpoint)
		hadoop_conf.set("fs.s3a.access.key", access_key)
		hadoop_conf.set("fs.s3a.secret.key", secret_key)
		hadoop_conf.set("fs.s3a.path.style.access", "true")
		hadoop_conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
		hadoop_conf.set("fs.s3a.connection.ssl.enabled", "false")
		return spark

	def get_spark(self) -> SparkSession:
		return self._spark
