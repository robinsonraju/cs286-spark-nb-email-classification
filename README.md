# cs286-spark-nb-email-classification

Email Classification as Spam/Ham using Naive Bayes Classifier on Apache Spark.

## Build and run

Use a JDK supported by Spark 3.5.7 (Java 8, 11, or 17) and Maven. Spark Core is a provided dependency: install Spark 3.5.7 built for Scala 2.12 on the machine or cluster that runs the classifier. The legacy Spark 1.4.1 runtime is incompatible with this build.

```sh
cd nb-email-classifier
mvn clean package
./run-standalone.sh
```

The launcher uses `spark-submit` from `SPARK_HOME/bin` when `SPARK_HOME` is set, or from `PATH` otherwise. It defaults to local execution with the included small dataset. Pass a CSV path as its first argument, and set `SPARK_MASTER` to run on a configured cluster.

The tests exercise word counting and full outer joins on a local Spark context. Both flat-map implementations use Spark's iterator API, and joins use Spark's own optional type. The unused Scala 2.10 MLlib dependency has been removed to prevent an incompatible Spark/Scala dependency graph.
