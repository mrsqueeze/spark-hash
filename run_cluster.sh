$SPARK_HOME/bin/spark-submit \
  --class "com.invincea.spark.hash.OpenPortApp" \
  --master yarn-client \
  --num-executors 80 \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 16 \
  target/spark-hash-*.jar $@
