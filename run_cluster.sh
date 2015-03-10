$SPARK_HOME/bin/spark-submit \
  --class "com.invincea.spark.hash.OpenPortApp" \
  --master yarn-client \
  --num-executors 80 \
  --driver-memory 4g \
  --executor-memory 8g \
  --executor-cores 16 \
  target/target/spark-hash-0.1.2.jar $@
