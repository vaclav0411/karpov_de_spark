import os
from pathlib import Path

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as f
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

# MySql
# host: rc1c-0yx6egw73a44epbu.mdb.yandexcloud.net
# database: spark
# user: developer
# password: NxZ58QAgu8bfnSbt


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


def agg_calc(spark: SparkSession) -> DataFrame:
    data_path = os.path.join(Path(__name__).parent, 'data', '*.csv')

    trip_fact = spark.read \
        .option("header", "true") \
        .schema(trips_schema) \
        .csv(data_path)

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


def create_dict(spark: SparkSession, header: list[str], data: list):
    """создание словаря"""
    df = spark.createDataFrame(data=data, schema=header)
    return df


def save_to_mysql(host: str, port: int, db_name: str, username: str, password: str, df: DataFrame, table_name: str):
    props = {
        'user': f'{username}',
        'password': f'{password}',
        'driver': 'com.mysql.cj.jdbc.Driver',
        "ssl": "true",
        "sslmode": "none",
    }

    df.write.jdbc(
        url=f'jdbc:mysql://{host}:{port}/{db_name}',
        table=table_name,
        properties=props)


def main(spark: SparkSession):
    vendor_dim = create_dict(spark, dim_columns, vendor_rows)
    payment_dim = create_dict(spark, dim_columns, payment_rows)
    rates_dim = create_dict(spark, dim_columns, rates_rows)

    datamart = agg_calc(spark)
    datamart.show()

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

    joined_datamart.show()

    mysql_host = 'rc1c-0yx6egw73a44epbu.mdb.yandexcloud.net'
    mysql_port = 3306
    mysql_db = 'spark'
    mysql_user = 'developer'
    mysql_password = 'NxZ58QAgu8bfnSbt'

    save_to_mysql(
        host=mysql_host,
        port=mysql_port,
        db_name=mysql_db,
        username=mysql_user,
        password=mysql_password,
        df=joined_datamart,
        table_name='spark.my_data_mart'
    )


if __name__ == '__main__':
    main(SparkSession.
         builder
         .config("spark.jars", "jars/mysql-connector-java-8.0.25.jar")
         .appName('My first spark job')
         .getOrCreate())
