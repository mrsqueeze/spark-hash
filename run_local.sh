$SPARK_HOME/bin/spark-submit \
  --class "com.invincea.spark.hash.OpenPortApp" \
  --master local[*] \
  --executor-memory 8G \
  --driver-memory 8G \
  target/spark-hash-0.1.2.jar $@
