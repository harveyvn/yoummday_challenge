import os
from pyspark.sql import SparkSession
from app.const import ENV

if __name__ == '__main__':
    master_url = os.getenv("SPARK_MASTER_URL", "local[*]")
    spark = SparkSession.builder.appName("PostgresS3Test").config("spark.jars.packages", "org.postgresql:postgresql:42.7.3").getOrCreate()

    if ENV == "PRD":
        jdbc_url = "jdbc:postgresql://listen_brainz_db_dm_container:5432/domain_model_db"
    else:
        jdbc_url = "jdbc:postgresql://localhost:5443/domain_model_db"

    connection_properties = {
        "user": "admin",
        "password": "password",
        "driver": "org.postgresql.Driver"
    }
    try:
        # This assumes you have a table like 'users' or can run a dummy query
        df_test = spark.read.jdbc(
            url=jdbc_url,
            table="(SELECT 1 AS test_col) AS test_query",
            properties=connection_properties
        )

        df_test.show()
        print("Connection test successful.")

    except Exception as e:
        print("Connection test failed.")
        print(f"Error: {e}")

    spark.stop()
