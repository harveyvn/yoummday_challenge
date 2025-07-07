from datetime import datetime
from pyspark.sql import DataFrame
from pyspark.sql.functions import col, current_timestamp
from app.const import ENV, BUCKET_NAME
from app.utils.common import get_data
from app.utils.logging import log
from app.utils.sql_utils import insert_spark_df_in_chunks


class Track:
    def __init__(self, bucket_name: str, s3_path: str):
        self._df, self._spark = get_data(bucket_name, s3_path)
        self._df = self._df.toDF("user_name", "listened_at", "musicbrainz-artist-id", "artist-name", "musicbrainz-track-id", "track-name")
        self._df = self._df.limit(300000)
        self._s3_transform_path: str = f"s3a://{bucket_name}/transformed/{datetime.today().strftime('%Y%m%d')}/tracks/"
        self._df_tracks: DataFrame = None

    def _transform(self):
        self._df.show(2)
        df_tracks = self._df.select(
            col("musicbrainz-track-id").alias("track_id"),
            col("track-name").alias("track_name"),
            col("musicbrainz-artist-id").alias("artist_id"),
            col("artist-name").alias("artist_name"),
        )
        df_tracks = df_tracks.withColumn("id", col("track_id"))
        df_tracks = df_tracks.withColumn("last_updated", current_timestamp())
        df_tracks = df_tracks.select("id", "track_name", "track_id", "artist_name", "artist_id", "last_updated")
        df_tracks.show(2)
        df_tracks = df_tracks.dropDuplicates(["id"])
        df_tracks = df_tracks.filter(col("id").isNotNull())
        self._df_tracks = df_tracks

    def _write_transform_to_s3(self):
        self._df_tracks.write.mode("overwrite").parquet(self._s3_transform_path)
        log.info(f'Upload transformed data to S3 Path: {self._s3_transform_path}')

    def _load(self):
        df = self._df_tracks.toPandas()
        insert_spark_df_in_chunks(spark_df=self._df_tracks, table_name="tracks", check_cols=["id"], batch_size=20000)
        log.info(f'Inserted {len(df)} tracks to db')

    def _validate_data(self):
        log.info("Running data quality checks for tracks...")
        is_valid = True

        df = self._df_tracks

        null_track_names = df.filter(col("track_name").isNull()).count()
        empty_track_names = df.filter(col("track_name") == "").count()

        null_artist_names = df.filter(col("artist_name").isNull()).count()
        empty_artist_names = df.filter(col("artist_name") == "").count()

        null_recording_ids = df.filter(col("track_id").isNull()).count()
        total_rows = df.count()
        unique_recording_ids = self._df.filter(col("musicbrainz-track-id").isNotNull()).select("musicbrainz-track-id").distinct().count()

        if null_track_names > 0:
            log.error(f"Data quality issue: {null_track_names} records have null track_name.")
            is_valid = False
        if empty_track_names > 0:
            log.error(f"Data quality issue: {empty_track_names} records have empty string as track_name.")
            is_valid = False

        if null_artist_names > 0:
            log.error(f"Data quality issue: {null_artist_names} records have null artist_name.")
            is_valid = False
        if empty_artist_names > 0:
            log.error(f"Data quality issue: {empty_artist_names} records have empty string as artist_name.")
            is_valid = False

        if null_recording_ids > 0:
            log.error(f"Data quality issue: {null_recording_ids} records have null track_id.")
            is_valid = False

        if total_rows != unique_recording_ids:
            log.error(f"Data quality issue: Duplicate track_ids found. Total rows: {total_rows}, Unique: {unique_recording_ids}")
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
    track_pipeline = Track(BUCKET_NAME, s3_url)
    track_pipeline.run()
