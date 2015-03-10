$SPARK_HOME/bin/spark-shell \
  --master local[*] \
  --executor-memory 8G \
  --driver-memory 8G \
  --jars target/spark-hash-0.1.2.jar $@
