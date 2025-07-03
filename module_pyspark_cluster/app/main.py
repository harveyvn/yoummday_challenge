from datetime import datetime

from pyspark.sql.functions import col, udf, current_timestamp
from pyspark.sql.types import StringType

from app.const import ENV
from app.utils.common import get_data
from app.utils.logging import log
from app.utils.sql_utils import insert_on_conflict_do_update


def _generate_uuid(text: str) -> str:
    import uuid
    return str(uuid.uuid5(uuid.NAMESPACE_URL, text))


if __name__ == '__main__':
    bucket = "datalake"
    s3_url = f"raw/{datetime.today().strftime('%Y%m%d')}/dataset.json"
    df, spark = get_data(bucket, s3_url)
    log.info(f'Raw Data: Found {df.count()} rows')

    uuid_udf = udf(_generate_uuid, StringType())
    df_users = df.select("user_name").distinct()
    df_users = df_users.withColumn("id", uuid_udf(col("user_name")))
    df_users = df_users.withColumn("last_updated", current_timestamp())
    df_users = df_users.select("id", "user_name", "last_updated")

    if ENV != "LOCAL":
        s3a_path = f"s3a://{bucket}/transformed/{datetime.today().strftime('%Y%m%d')}/users/"
        df_users.write.mode("overwrite").parquet(s3a_path)
        log.info(f'Upload transformed data to S3 Path: {s3a_path}')

    df = df_users.toPandas()
    insert_on_conflict_do_update(df=df, table_name="users", check_cols=["id"], batch=100)
    log.info(f'Inserted {len(df)} users')
