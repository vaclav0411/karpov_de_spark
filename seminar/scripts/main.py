import os
import sys
from pathlib import Path

from typing import List

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# MySql
# MYSQL_HOST = 'rc1c-v4f7g6yol4srzrqf.mdb.yandexcloud.net'
# MYSQL_PORT = 3306
# MYSQL_DATABASE = 'lab4'
# MYSQL_TABLE = 'mega_datamart'
# MYSQL_USER = 'spark'
# MYSQL_PASSWORD = 'sparkspark'

# поля справочника
dim_columns = ['id', 'name']

vendor_rows = [
    (1, 'Creative Mobile Technologies, LLC'),
    (2, 'VeriFone Inc'),
]

rates_rows = [
    (1, 'Standard rate'),
    (2, 'JFK'),
    (3, 'Newark'),
    (4, 'Nassau or Westchester'),
    (5, 'Negotiated fare'),
    (6, 'Group ride'),
]

payment_rows = [
    (1, 'Credit card'),
    (2, 'Cash'),
    (3, 'No charge'),
    (4, 'Dispute'),
    (5, 'Unknown'),
    (6, 'Voided trip'),
]

trips_schema = StructType([
    StructField('vendor_id', StringType(), True),
    StructField('tpep_pickup_datetime', TimestampType(), True),
    StructField('tpep_dropoff_datetime', TimestampType(), True),
    StructField('passenger_count', IntegerType(), True),
    StructField('trip_distance', DoubleType(), True),
    StructField('ratecode_id', IntegerType(), True),
    StructField('store_and_fwd_flag', StringType(), True),
    StructField('pulocation_id', IntegerType(), True),
    StructField('dolocation_id', IntegerType(), True),
    StructField('payment_type', IntegerType(), True),
    StructField('fare_amount', DoubleType(), True),
    StructField('extra', DoubleType(), True),
    StructField('mta_tax', DoubleType(), True),
    StructField('tip_amount', DoubleType(), True),
    StructField('tolls_amount', DoubleType(), True),
    StructField('improvement_surcharge', DoubleType(), True),
    StructField('total_amount', DoubleType(), True),
    StructField('congestion_surcharge', DoubleType()),
])


def agg_calc(path: str, spark: SparkSession) -> DataFrame:
    trip_fact = spark.read \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(path)

    datamart = trip_fact \
        .where(trip_fact['vendor_id'].isNotNull()) \
        .groupBy(trip_fact['vendor_id'],
                 trip_fact['payment_type'],
                 trip_fact['ratecode_id'],
                 f.to_date(trip_fact['tpep_pickup_datetime']).alias('dt')
                 ) \
        .agg(f.sum(trip_fact['total_amount']).alias('sum_amount'), f.avg(trip_fact['tip_amount']).alias("avg_tips")) \
        .select(f.col('dt'),
                f.col('vendor_id'),
                f.col('payment_type'),
                f.col('ratecode_id'),
                f.col('sum_amount'),
                f.col('avg_tips')) \
        .orderBy(f.col('dt').desc(), f.col('vendor_id'))

    return datamart


def create_dict(spark: SparkSession, header: List[str], data: list):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df


def main(spark: SparkSession):
    input_data = sys.argv[1]
    output_directory = sys.argv[2]

    vendor_dim = create_dict(spark, dim_columns, vendor_rows)
    payment_dim = create_dict(spark, dim_columns, payment_rows)
    rates_dim = create_dict(spark, dim_columns, rates_rows)

    datamart = agg_calc(input_data, spark)

    joined_datamart = datamart \
        .join(other=vendor_dim, on=vendor_dim['id'] == f.col('vendor_id'), how='inner') \
        .join(other=payment_dim, on=payment_dim['id'] == f.col('payment_type'), how='inner') \
        .join(other=rates_dim, on=rates_dim['id'] == f.col('ratecode_id'), how='inner') \
        .select(f.col('dt'),
                f.col('vendor_id'), f.col('payment_type'), f.col('ratecode_id'), f.col('sum_amount'),
                f.col('avg_tips'),
                rates_dim['name'].alias('rate_name'), vendor_dim['name'].alias('vendor_name'),
                payment_dim['name'].alias('payment_name'),
                )

    # joined_datamart.show(truncate=False, n=100000)

    joined_datamart.write.mode('overwrite').csv(output_directory)

    # save_to_mysql(
    #     host=MYSQL_HOST,
    #     port=MYSQL_PORT,
    #     db_name=MYSQL_DATABASE,
    #     username=MYSQL_USER,
    #     password=MYSQL_PASSWORD,
    #     df=joined_datamart,
    #     table_name=f'{MYSQL_DATABASE}.{MYSQL_TABLE}'
    # )

    print('end')


if __name__ == '__main__':
    main(SparkSession
         .builder
         .appName('My first spark job')
         .config("spark.ui.prometheus.enabled", "true")
         .getOrCreate())

# .config("spark.jars", "./practice4/jars/mysql-connector-java-8.0.25.jar")
