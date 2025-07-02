import os
from pyspark.sql import SparkSession


def main():
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    app_name = "HelloWorld"

    # Initialize SparkSession
    spark = SparkSession.builder.appName(app_name).master(master_url).getOrCreate()

    # Create an RDD containing numbers from 1 to 1000
    numbers_rdd = spark.sparkContext.parallelize(range(1, 1000))

    # Count the elements in the RDD
    count = numbers_rdd.count()

    print(f"Count of numbers from 1 to 1000 is: {count}")

    # Stop the SparkSession
    spark.stop()


if __name__ == "__main__":
    main()
