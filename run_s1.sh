
/usr/local/spark/bin/spark-submit --master spark://ec2-35-155-195-242.us-west-2.compute.amazonaws.com:7077 \
--packages org.apache.spark:spark-streaming-kafka-0-8_2.11:2.0.2 \
--num-executors 4 \
--executor-cores 3 \
--executor-memory 2G \
--conf spark.scheduler.mode=FAIR \
--conf spark.default.parallelism=2 \
~/Insight-DE19A/s1yelp.py