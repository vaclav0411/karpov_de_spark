import sys

from pyspark.sql import SparkSession

import os

from pyspark.sql import functions as f
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import collect_set, to_json, struct
from pyspark.sql.types import StructType, StructField, StringType, TimestampType, IntegerType, DoubleType

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


def foreach_batch_function(df: DataFrame, epoch_id):
    df.write.mode("append").json("output_report")


def main(spark: SparkSession):
    kafka_broker = sys.argv[1]
    output_directory = sys.argv[2]

    jsonOptions = {"timestampFormat": "yyyy-MM-dd'T'HH:mm:ss.sss'Z'"}

    fields = list(map(lambda x: f"json_message.{x.name}", trips_schema.fields))

    df = spark \
        .readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "51.250.98.191:29092") \
        .option("subscribe", "taxi") \
        .option("startingOffsets", "latest") \
        .load() \
        .select(f.from_json(f.col("value").cast("string"), trips_schema, jsonOptions).alias("json_message")) \
        .select(fields)

    # .option("maxOffsetsPerTrigger", 1000) \

    # пишем на диск
    writer = df \
        .writeStream \
        .trigger(processingTime='10 seconds') \
        .option("path", output_directory) \
        .option('checkpointLocation', 's3a://seminar-data/checkpoint2') \
        .outputMode('append') \
        .start()

    # .foreachBatch(foreach_batch_function) \

    # считаем витрину
    # mart = df.groupBy('payment_type', f.to_date('tpep_pickup_datetime').alias('dt')).agg(
    #     f.count(f.col('*')).alias('cnt')) \
    #     .join(
    #     other=create_dict(spark, dim_columns, payment_rows),
    #     on=f.col('payment_type') == f.col('id'),
    #     how='inner') \
    #     .select(f.col('name'), f.col('cnt'), f.col('dt')) \
    #     .orderBy(f.col('name'), f.col('dt')) \
    #     .select(to_json(struct("name", "cnt", "dt")).alias('value'))

    writer.awaitTermination()


if __name__ == '__main__':
    main(SparkSession.
         builder
         .appName("streaming_job")
         .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.2.0")
         .config("spark.ui.prometheus.enabled", "true")
         .getOrCreate())
