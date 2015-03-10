$SPARK_HOME/bin/spark-shell \
  --master yarn-client \
  --num-executors 8 \
  --driver-memory 4g \
  --executor-memory 400g \
  --executor-cores 60 \
  --jars target/spark-hash-0.1.2.jar $@
