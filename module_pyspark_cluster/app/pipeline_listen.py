from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, current_timestamp, concat_ws, from_unixtime, to_timestamp, to_date
from pyspark.sql.types import StringType
from app.const import ENV, BUCKET_NAME
from app.utils.common import get_data
from app.utils.logging import log
from app.utils.sql_utils import insert_on_conflict_do_update


class Listen:
    def __init__(self, bucket_name: str, s3_path: str):
        self._df, self._spark = get_data(bucket_name, s3_path)
        self._s3_transform_path: str = f"s3a://{bucket_name}/transformed/{datetime.today().strftime('%Y%m%d')}/listens/"
        self._df_listens: DataFrame = None

    @staticmethod
    def _generate_uuid(text: str) -> str:
        import uuid
        return str(uuid.uuid5(uuid.NAMESPACE_URL, text))

    def _transform(self):
        df_listens = self._df.select(
            col("user_name").alias("user_name"),
            col("recording_msid").alias("track_id"),
            col("listened_at").alias("listened_unix_timestamp"),
        )
        uuid_udf = udf(self._generate_uuid, StringType())
        df_listens = df_listens.withColumn("id", uuid_udf(concat_ws(" ", col("user_name"), col("track_id"), col("listened_unix_timestamp"))))
        df_listens = df_listens.withColumn("user_id", uuid_udf(col("user_name")))
        df_listens = df_listens.withColumn("listened_at", to_timestamp(from_unixtime(col("listened_unix_timestamp"))))
        df_listens = df_listens.withColumn("date", to_date(col("listened_at")))
        df_listens = df_listens.withColumn("last_updated", current_timestamp())
        df_listens = df_listens.select("id", "user_id", "track_id", "listened_at", "date", "last_updated")
        self._df_listens = df_listens

    def _write_transform_to_s3(self):
        self._df_listens.write.mode("overwrite").parquet(self._s3_transform_path)
        log.info(f'Upload transformed data to S3 Path: {self._s3_transform_path}')

    def _load(self):
        df = self._df_listens.toPandas()
        insert_on_conflict_do_update(df=df, table_name="listens", batch=5000)
        log.info(f'Inserted {len(df)} listens to db')

    def _validate_data(self):
        log.info("Running data quality checks for listens...")
        is_valid = True

        df = self._df_listens

        null_track_id = df.filter(col("track_id").isNull()).count()
        empty_track_id = df.filter(col("track_id") == "").count()

        null_user_id = df.filter(col("user_id").isNull()).count()
        empty_user_id = df.filter(col("user_id") == "").count()

        null_listened_at = df.filter(col("listened_at").isNull()).count()
        empty_listened_at = df.filter(col("listened_at") == "").count()

        null_listens_ids = df.filter(col("id").isNull()).count()
        total_rows = df.count()
        unique_recording_ids = df.select("id").distinct().count()

        if null_track_id > 0:
            log.error(f"Data quality issue: {null_track_id} track_ids have null track_id.")
            is_valid = False
        if empty_track_id > 0:
            log.error(f"Data quality issue: {empty_track_id} track_ids have empty string as track_id.")
            is_valid = False

        if null_user_id > 0:
            log.error(f"Data quality issue: {null_user_id} records have null user_name.")
            is_valid = False
        if empty_user_id > 0:
            log.error(f"Data quality issue: {empty_user_id} records have empty string as user_name.")
            is_valid = False

        if null_listened_at > 0:
            log.error(f"Data quality issue: {null_listened_at} records have null listened_at.")
            is_valid = False
        if empty_listened_at > 0:
            log.error(f"Data quality issue: {empty_listened_at} records have empty string as listened_at.")
            is_valid = False

        if null_listens_ids > 0:
            log.error(f"Data quality issue: {null_listens_ids} listens have null track_id, user_id, and listened_at.")
            is_valid = False

        if total_rows != unique_recording_ids:
            log.error(f"Data quality issue: Duplicate listen_ids found. Total rows: {total_rows}, Unique: {unique_recording_ids}")
            is_valid = False

        if total_rows == 0:
            raise ValueError("Data quality issue: No track records to process.")

        if is_valid is False:
            raise ValueError("Data quality checks failed for tracks.")

        log.info("Data quality checks passed for tracks.")

    def run(self):
        log.info(f'Raw Data: Found {self._df.count()} rows')
        self._transform()
        self._validate_data()
        if ENV != "LOCAL":
            self._write_transform_to_s3()
        self._load()
        self._spark.stop()


if __name__ == '__main__':
    s3_url = f"raw/{datetime.today().strftime('%Y%m%d')}/dataset.json"
    track_pipeline = Listen(BUCKET_NAME, s3_url)
    track_pipeline.run()
