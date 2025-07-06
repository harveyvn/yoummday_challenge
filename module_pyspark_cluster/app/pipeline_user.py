from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType
from app.const import ENV, BUCKET_NAME
from app.utils.common import get_data
from app.utils.logging import log
from app.utils.sql_utils import insert_on_conflict_do_update


class User:
    def __init__(self, bucket_name: str, s3_path: str):
        self._df, self._spark = get_data(bucket_name, s3_path)
        self._s3_transform_path: str = f"s3a://{bucket_name}/transformed/{datetime.today().strftime('%Y%m%d')}/users/"
        self._df_users: DataFrame = None

    @staticmethod
    def _generate_uuid(text: str) -> str:
        import uuid
        return str(uuid.uuid5(uuid.NAMESPACE_URL, text))

    def _transform(self):
        uuid_udf = udf(self._generate_uuid, StringType())
        df_users = self._df.select("user_name").distinct()
        df_users = df_users.withColumn("id", uuid_udf(col("user_name")))
        df_users = df_users.withColumn("last_updated", current_timestamp())
        df_users = df_users.select("id", "user_name", "last_updated")
        self._df_users = df_users

    def _write_transform_to_s3(self):
        self._df_users.write.mode("overwrite").parquet(self._s3_transform_path)
        log.info(f'Upload transformed data to S3 Path: {self._s3_transform_path}')

    def _load(self):
        df = self._df_users.toPandas()
        insert_on_conflict_do_update(df=df, table_name="users", check_cols=["id"], batch=100)
        log.info(f'Inserted {len(df)} users to db')

    def _validate_data(self):
        log.info("Running data quality checks...")

        null_usernames = self._df_users.filter(col("user_name").isNull()).count()
        empty_usernames = self._df_users.filter(col("user_name") == "").count()
        total_rows = self._df_users.count()
        unique_usernames = self._df_users.select("user_name").distinct().count()

        if null_usernames > 0:
            log.warn(f"Data quality issue: {null_usernames} records have null user_name.")
        if empty_usernames > 0:
            log.warn(f"Data quality issue: {empty_usernames} records have empty string as user_name.")
        if total_rows != unique_usernames:
            log.warn(f"Data quality issue: Duplicate user_names found. Total rows: {total_rows}, Unique: {unique_usernames}")
        if total_rows == 0:
            raise ValueError("Data quality issue: No user records to process.")

        log.info("Data quality checks passed.")

    def run(self):
        log.info(f'Raw Data: Found {self._df.count()} rows')
        self._transform()
        self._validate_data()
        if ENV != "LOCAL":
            self._write_transform_to_s3()
        self._load()


if __name__ == '__main__':
    s3_url = f"raw/{datetime.today().strftime('%Y%m%d')}/dataset.txt"
    user_pipeline = User(BUCKET_NAME, s3_url)
    user_pipeline.run()
