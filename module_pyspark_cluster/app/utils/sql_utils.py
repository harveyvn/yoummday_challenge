import os
from contextlib import contextmanager

import numpy as np
import pandas as pd
import sqlalchemy
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker, scoped_session

from pyspark.sql import Row
from itertools import islice

ENV = os.getenv('ENV', 'LOCAL').upper()
DB_USER = os.getenv('DB_USER', 'admin')
DB_PASS = os.getenv('DB_PASS', 'password')
DB_HOST = os.getenv('DB_HOST', 'localhost')
DB_PORT = os.getenv('DB_PORT', '5443')
DB_NAME = os.getenv('DB_NAME', 'domain_model_db')

db_uri = f'postgresql://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}'

engine = create_engine(db_uri, pool_pre_ping=True)
Session = scoped_session(sessionmaker(autocommit=False, autoflush=False, bind=engine))


@contextmanager
def session_scope():
    """Provide a transactional scope around a series of operations."""

    session = Session()
    try:
        yield session
        session.commit()
    except:
        session.rollback()
        raise
    finally:
        session.close()


def read_from_sql_statement(statement: str) -> pd.DataFrame:
    with session_scope() as session:
        result = session.execute(statement)
        columns = result.cursor.description
        df = pd.DataFrame(result.fetchall(), columns=[c.name for c in columns])
    return df


def get_primary_keys_of_table(table_name: str) -> list:
    table_name = table_name.replace('"', '')
    statement = f"select \
                    a.attname as column_name \
                    from \
                        pg_class t, \
                        pg_class i, \
                        pg_index ix, \
                        pg_attribute a \
                    where \
                        t.oid = ix.indrelid \
                        and i.oid = ix.indexrelid \
                        and a.attrelid = t.oid \
                        and a.attnum = ANY(ix.indkey) \
                        and t.relkind = 'r' \
                        and t.relname = '{table_name}'"

    keys = read_from_sql_statement(statement=statement)
    keys.drop_duplicates(inplace=True)
    return keys['column_name'].to_list()


def build_values(entry):
    values_clean = []
    for value in entry:
        if value is not None:
            try:
                values_clean.append(value.replace("'", "''").replace(':', '\\:'))  # escape colon (binding parameter)
            except Exception:
                values_clean.append(value)
        else:
            values_clean.append(value)
    values = (", ".join(["'{e}'".format(e=e) for e in values_clean]))
    klam_values = "({})".format(values)
    return klam_values


def insert_on_conflict_do_update(df: pd.DataFrame, table_name, schema='public', check_cols=None, batch=10):
    df = df.replace([np.nan, 'NaT', 'NaN'], 'null')
    insert_dict = df.values.tolist()
    column_names_list = list(df.columns.values)
    column_names_str = ",".join(['\"{}\"'.format(col_name) for col_name in column_names_list])
    if not check_cols:
        check_cols = get_primary_keys_of_table(table_name=table_name)

    for chunk in chunks(list_obj=insert_dict, batch_size=batch):
        insert_stmt_str = "INSERT INTO {schema}.{table_name} ({column_names}) VALUES ".format(schema=schema,
                                                                                              table_name=table_name,
                                                                                              column_names=column_names_str)
        values = []
        for entry in chunk:
            values.append(build_values(entry))
        values = ','.join(values)
        values = values.strip('[]')
        values = values.replace('"', '')
        insert_stmt_str = "{} {}".format(insert_stmt_str, values)

        # get primary keys of table
        pkey = (", ".join(["{e}".format(e=e) for e in check_cols]))
        excluded = " Set "
        for col in column_names_list:
            excluded = '{excluded} "{col_name_left_side}"=EXCLUDED."{col_name_right_side}",'.format(
                excluded=excluded,
                col_name_left_side=col,
                col_name_right_side=col)

        excluded = excluded[:-1]
        insert_stmt_str = '{insert} ON CONFLICT ({pkey}) DO UPDATE {excluded_stmt};'.format(insert=insert_stmt_str,
                                                                                            pkey=pkey,
                                                                                            excluded_stmt=excluded)
        insert_stmt_str = insert_stmt_str.replace("'null'", "null")
        with session_scope() as session:
            session.execute(sqlalchemy.text(insert_stmt_str))


def chunks(list_obj, batch_size):
    """Yield successive n-sized chunks from l."""
    for i in range(0, len(list_obj), batch_size):
        yield list_obj[i:i + batch_size]


def insert_spark_df_in_chunks(spark_df, table_name, check_cols, batch_size=10):
    def chunked_iterator(iterator, batch_size):
        """Yield successive chunks from an iterator."""
        while True:
            chunk = list(islice(iterator, batch_size))
            if not chunk:
                break
            yield chunk

    total_inserted = 0

    # Get local iterator over the Spark DataFrame rows
    row_iterator = spark_df.toLocalIterator()

    for chunk in chunked_iterator(row_iterator, batch_size):
        # Convert chunk (list of Rows) to Pandas DataFrame
        pandas_df = pd.DataFrame([row.asDict() for row in chunk])

        # Insert into DB
        insert_on_conflict_do_update(df=pandas_df, check_cols=check_cols, table_name=table_name, batch=batch_size)

        total_inserted += len(pandas_df)
