from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, udf, current_timestamp, concat_ws, to_date
from pyspark.sql.types import StringType
from app.const import ENV, BUCKET_NAME
from app.utils.common import get_data
from app.utils.logging import log
from app.utils.sql_utils import insert_spark_df_in_chunks


class Listen:
    def __init__(self, bucket_name: str, s3_path: str):
        self._df, self._spark = get_data(bucket_name, s3_path)
        self._df = self._df.toDF("user_name", "listened_at", "musicbrainz-artist-id", "artist-name", "musicbrainz-track-id", "track-name")
        self._s3_transform_path: str = f"s3a://{bucket_name}/transformed/{datetime.today().strftime('%Y%m%d')}/listens/"
        self._df_listens: DataFrame = None

    @staticmethod
    def _generate_uuid(text: str) -> str:
        import uuid
        return str(uuid.uuid5(uuid.NAMESPACE_URL, text))

    def _transform(self):
        self._df.show(2)
        df_listens = self._df.select(
            col("user_name").alias("user_name"),
            col("musicbrainz-track-id").alias("track_id"),
            col("listened_at").alias("listened_at"),
        )
        uuid_udf = udf(self._generate_uuid, StringType())
        df_listens = df_listens.withColumn("id", uuid_udf(concat_ws(" ", col("user_name"), col("track_id"), col("listened_at"))))
        df_listens = df_listens.withColumn("user_id", uuid_udf(col("user_name")))
        df_listens = df_listens.withColumn("date", to_date(col("listened_at")))
        df_listens = df_listens.withColumn("last_updated", current_timestamp())
        df_listens = df_listens.select("id", "user_id", "track_id", "listened_at", "date", "last_updated")
        df_listens.show(2)
        df_listens = df_listens.dropDuplicates(["id"])
        df_listens = df_listens.filter(col("track_id").isNotNull())
        df_listens = df_listens.filter(col("listened_at").isNotNull())
        df_listens.show(2)
        self._df_listens = df_listens

    def _write_transform_to_s3(self):
        self._df_listens.write.mode("overwrite").parquet(self._s3_transform_path)
        log.info(f'Upload transformed data to S3 Path: {self._s3_transform_path}')

    def _load(self):
        insert_spark_df_in_chunks(spark_df=self._df_listens, table_name="listens", check_cols=None, batch_size=500000)
        log.info(f'Inserted {self._df_listens.count()} listens to db')

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
    s3_url = f"raw/{datetime.today().strftime('%Y%m%d')}/userid-timestamp-artid-artname-traid-traname.tsv"
    track_pipeline = Listen(BUCKET_NAME, s3_url)
    track_pipeline.run()
