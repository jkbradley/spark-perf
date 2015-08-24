# Spark Performance Tests

[![Build Status](https://travis-ci.org/databricks/spark-perf.svg?branch=master)](https://travis-ci.org/databricks/spark-perf)

This is a performance testing framework for [Apache Spark](http://spark.apache.org) 1.0+.

## Features

- Suites of performance tests for Spark, PySpark, Spark Streaming, and MLlib.
- Parameterized test configurations:
   - Sweeps sets of parameters to test against multiple Spark and test configurations.
- Automatically downloads and builds Spark:
   - Maintains a cache of successful builds to enable rapid testing against multiple Spark versions.
- [...]

For questions, bug reports, or feature requests, please [open an issue on GitHub](https://github.com/databricks/spark-perf/issues).

## Coverage

- Spark Core RDD
  - list coming soon
- SQL and DataFrames
  - coming soon
- Machine Learning
  - glm-regression: Generalized Linear Regression Model
  - glm-classification: Generalized Linear Classification Model
  - naive-bayes: Naive Bayes
  - naive-bayes-bernoulli: Bernoulli Naive Bayes
  - decision-tree: Decision Tree
  - als: Alternating Least Squares
  - kmeans: K-Means clustering
  - gmm: Gaussian Mixture Model
  - svd: Singular Value Decomposition
  - pca: Principal Component Analysis
  - summary-statistics: Summary Statistics (min, max, ...)
  - block-matrix-mult: Matrix Multiplication
  - pearson: Pearson's Correlation
  - spearman: Spearman's Correlation
  - chi-sq-feature/gof/mat: Chi-square Tests
  - word2vec: Word2Vec distributed presentation of words
  - fp-growth: FP-growth frequent item sets
  - python-glm-classification: Generalized Linear Classification Model
  - python-glm-regression: Generalized Linear Regression Model
  - python-naive-bayes: Naive Bayes
  - python-als: Alternating Least Squares
  - python-kmeans: K-Means clustering
  - python-pearson: Pearson's Correlation
  - python-spearman: Spearman's Correlation


## Dependencies

The `spark-perf` scripts require Python 2.7+.  If you're using an earlier version of Python, you may need to install the `argparse` library using `easy_install argparse`.

Support for automatically building Spark requires Maven.  On `spark-ec2` clusters, this can be installed using the `./bin/spark-ec2/install-maven` script from this project.


## Configuration

To configure `spark-perf`, copy `config/config.py.template` to `config/config.py` and edit that file.  See `config.py.template` for detailed configuration instructions.  After editing `config.py`, execute `./bin/run` to run performance tests.  You can pass the `--config` option to use a custom configuration file.

The following sections describe some additional settings to change for certain test environments:

### Running locally

1. Set up local SSH server/keys such that `ssh localhost` works on your machine without a password.
2. Set config.py options that are friendly for local execution:

   ```
   SPARK_HOME_DIR = /path/to/your/spark
   SPARK_CLUSTER_URL = "spark://%s:7077" % socket.gethostname()
   SCALE_FACTOR = .05
   SPARK_DRIVER_MEMORY = 512m
   spark.executor.memory = 2g
   ```
3. Uncomment at least one `SPARK_TESTS` entry.

### Running on an existing Spark cluster
1. SSH into the machine hosting the standalone master
2. Set config.py options:

   ```
   SPARK_HOME_DIR = /path/to/your/spark/install
   SPARK_CLUSTER_URL = "spark://<your-master-hostname>:7077"
   SCALE_FACTOR = <depends on your hardware>
   SPARK_DRIVER_MEMORY = <depends on your hardware>
   spark.executor.memory = <depends on your hardware>
   ```
3. Uncomment at least one `SPARK_TESTS` entry.

### Running on a spark-ec2 cluster with a custom Spark version
1. Launch an EC2 cluster with [Spark's EC2 scripts](https://spark.apache.org/docs/latest/ec2-scripts.html).
2. Set config.py options:

   ```
   USE_CLUSTER_SPARK = False
   SPARK_COMMIT_ID = <what you want test>
   SCALE_FACTOR = <depends on your hardware>
   SPARK_DRIVER_MEMORY = <depends on your hardware>
   spark.executor.memory = <depends on your hardware>
   ```
3. Uncomment at least one `SPARK_TESTS` entry.

# Running MLLib perf tests as a library
This skips the cluster setup / SparkContext management taken care of by
`bin/run` and `lib/sparkperf/main.py`, assuming that you have a cluster
running Spark you can add JARs into the classpath.

This is useful if you are running perf tests in a managed environment
(e.g.  Databricks notebooks).

Configs for the JAR are managed in
`mllib-tests/v1p5/src/main/scala/mllib/perf/Config.scala`.

1. `cd mllib-tests/ && sbt/sbt -Dspark.version=1.5.0-SNAPSHOT clean assembly`
2. Upload `mllib-tests/target/mllib-perf-tests-assembly.jar` library to
   Databricks and attach to cluster.
3. Create a `Config` object with desired `SCALE_FACTOR` and `numTrials`
   ```scala
   val config = new Config(SCALE_FACTOR=SCALE_FACTOR, numTrials=numTrials)``
   ```
   (Optional) Modify `config.MLLIB_TESTS` to only run the tests you want
   ```scala
   val testsToRun = Set(
     "decision-tree",
     "fp-growth",
     "kmeans",
     "word2vec"
   )
   config.MLLIB_TESTS = config.MLLIB_TESTS.filter {
     case TestInstance(shortName, _, _) => testsToRun.contains(shortName)
   }
   ```
4. Run the tests, passing the `sqlContext`, `config`, and a filepath (or
   DBFS mount to S3 bucket) for writing DataFrame results to:
   ```scala
   val (failedTests, testResults) = PerfSuite.runSuite(sqlContext, config, s"/$mountName")
   ```

## License

This project is licensed under the Apache 2.0 License. See LICENSE for full license text.

