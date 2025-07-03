import os
from pyspark.sql import SparkSession
from utils.logging import log


def main():
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    app_name = "HelloWorld"

    spark = SparkSession.builder.appName(app_name).master(master_url).getOrCreate()
    numbers_rdd = spark.sparkContext.parallelize(range(1, 1000))

    count = numbers_rdd.count()

    log.info(f"Count of numbers from 1 to 1000 is: {count}")

    spark.stop()
    log.info(f"Stopped SparkSession")


if __name__ == "__main__":
    main()
