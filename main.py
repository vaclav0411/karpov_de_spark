from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as f
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# поля справочника
dim_columns = ["id", "name"]

vendor_rows = [
    (1, "Creative Mobile Technologies, LLC"),
    (2, "VeriFone Inc"),
]

rates_rows = [
    (1, "Standard rate"),
    (2, "JFK"),
    (3, "Newark"),
    (4, "Nassau or Westchester"),
    (5, "Negotiated fare"),
    (6, "Group ride"),
]

payment_rows = [
    (1, "Credit card"),
    (2, "Cash"),
    (3, "No charge"),
    (4, "Dispute"),
    (5, "Unknown"),
    (6, "Voided trip"),
]

trips_schema = StructType(
    [
        StructField("vendor_id", StringType(), True),
        StructField("tpep_pickup_datetime", TimestampType(), True),
        StructField("tpep_dropoff_datetime", TimestampType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("trip_distance", DoubleType(), True),
        StructField("ratecode_id", IntegerType(), True),
        StructField("store_and_fwd_flag", StringType(), True),
        StructField("pulocation_id", IntegerType(), True),
        StructField("dolocation_id", IntegerType(), True),
        StructField("payment_type", IntegerType(), True),
        StructField("fare_amount", DoubleType(), True),
        StructField("extra", DoubleType(), True),
        StructField("mta_tax", DoubleType(), True),
        StructField("tip_amount", DoubleType(), True),
        StructField("tolls_amount", DoubleType(), True),
        StructField("improvement_surcharge", DoubleType(), True),
        StructField("total_amount", DoubleType(), True),
        StructField("congestion_surcharge", DoubleType()),
    ]
)


def save_to_postgres(
    host: str,
    port: int,
    db_name: str,
    username: str,
    password: str,
    df: DataFrame,
    table_name: str,
):
    props = {
        "user": f"{username}",
        "password": f"{password}",
        "driver": "org.postgresql.Driver",
    }

    df.write.mode("overwrite").jdbc(
        url=f"jdbc:postgresql://{host}:{port}/{db_name}",
        table=table_name,
        properties=props,
    )


def main(spark: SparkSession):
    data_path = "data/yellow_tripdata_2020-04.csv"
    trip_fact = spark.read.option("header", "true").schema(trips_schema).csv(data_path)

    trip_fact = (
        trip_fact.filter("total_amount >= 0")
        .withColumn("week", f.weekofyear("tpep_pickup_datetime"))
        .withColumn("tips_to_total", f.col("tip_amount") / f.col("total_amount"))
    )

    result = trip_fact.groupby("week", "payment_type").agg(
        f.mean("total_amount").alias("avg_check"),
        f.mean("tips_to_total").alias("avg_tips_to_total"),
    )

    gp_host = "greenplum.lab.karpov.courses"
    gp_port = 6432
    gp_db = "students"
    gp_user = "student"
    gp_password = "Wrhy96_09iPcreqAS"

    save_to_postgres(
        host=gp_host,
        port=gp_port,
        db_name=gp_db,
        username=gp_user,
        password=gp_password,
        df=result,
        table_name="spark_practice_samsonov",
    )


if __name__ == "__main__":
    main(
        SparkSession.builder.config("spark.jars", "jars/postgresql-42.3.1.jar")
        .appName("spark-homework")
        .getOrCreate()
    )
