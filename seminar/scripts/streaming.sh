export SPARK_VERSION=3.0.2
export SCALA_VERSION=2.12

spark-submit \
--conf "spark.driver.extraJavaOptions=-javaagent:jmx_prometheus_javaagent-0.16.1.jar=8080:spark.yml" \
--conf "spark.metrics.conf=./metrics.conf" \
--master yarn \
--deploy-mode client \
--executor-memory 4G \
--num-executors 2 \
--conf spark.hadoop.fs.s3a.endpoint=storage.yandexcloud.net \
streaming_job.py '51.250.98.191:29092' 's3a://seminar-data/streaming_data'
