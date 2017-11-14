spark-submit --class com.onepointltd.kudu.SparkDemo \
--master local --deploy-mode client --executor-memory 1g \
--name spark_kudu_demo --conf "spark.app.id=spark_kudu_demo" \
kudu-spark-1.0-SNAPSHOT-jar-with-dependencies.jar