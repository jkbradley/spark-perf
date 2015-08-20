package mllib.perf


import java.util.Calendar

import org.apache.spark.SparkContext

/**
 * Created by fliang on 8/20/15.
 */
case class TestInstance(shortName: String, mainClass: String, scaleFactor: Double, options: Seq[TestOption])

sealed trait TestOption {
  def to_array(scale_factor: Double): Seq[String]
}

case class VariableOption(name: String, vals: Seq[Any], can_scale: Boolean = false) extends TestOption {
  def scaled_vals(scale_factor: Double): Seq[Any] = {
    if (can_scale) vals.map(v => math.max(1, (v.toString.toDouble * scale_factor).toInt))
    else vals
  }

  def to_array(scale_factor: Double = 1.0): Seq[String] = {
    scaled_vals(scale_factor).map(v => s"--${this.name}=${v}")
  }
}

case class ConstantOption(name: String) extends TestOption {
  def to_array(scale_factor: Double = 1.0): Seq[String] = Seq(this.name)
}

object PerfSuite {
  val OUTPUT_DIVIDER_STRING = "-" * 68

  def runSuite(sc: SparkContext) = {
    val test_group_name = "MLlib-Tests"
    val tests_to_run = Config.MLLIB_TESTS
    val num_tests_to_run = tests_to_run.size

    println(OUTPUT_DIVIDER_STRING)
    println(s"Running $num_tests_to_run tests in $test_group_name")
    var failed_tests = Seq[String]()

    for (
      TestInstance(short_name, main_class_or_script, scale_factor, opt_sets: Seq[TestOption]) <- tests_to_run
    ) {
      println(OUTPUT_DIVIDER_STRING)
      // Run a test for all combinations of the VariableOptions given, then capture
      // and print the output.
      val opt_set_arrays = opt_sets.map(i => i.to_array(scale_factor))
      for (opt_list <- opt_set_arrays.reduceLeft((xs, ys) => for {x <- xs; y <- ys} yield x + " " ++ y)) {
        val args = opt_list.split(" ")
        val testName = args(0)
        val perfTestArgs = args.slice(1, args.length)
        println(s"Running: ${testName} ${perfTestArgs.mkString(" ")}")
        try {
          val results = TestRunner.run(sc, testName, perfTestArgs)
          val result_string = (results \ "results").toString
          println(OUTPUT_DIVIDER_STRING)
          println("Result: " + result_string)
          println(OUTPUT_DIVIDER_STRING)
        } catch {
          case (e: Exception) =>
            println(OUTPUT_DIVIDER_STRING)
            failed_tests ++= Seq(short_name)
            println(s"*** FAILED ***")
            e.printStackTrace()
            println(OUTPUT_DIVIDER_STRING)
        }
      }
    }
    println(s"\nFinished running $num_tests_to_run tests in $test_group_name.")
    println(s"\nNumber of failed tests: ${failed_tests.size}, failed tests: ${failed_tests.mkString(",")}")
    print(OUTPUT_DIVIDER_STRING)
  }
}

object Config {
  val SPARK_VERSION = 1.5
  //  val MLLIB_OUTPUT_FILENAME = s"results/mllib_perf_output_${SPARK_VERSION}_${Calendar.getInstance().getTime}"

  // ============================
  //  Test Configuration Options
  // ============================

  // The default values configured below are appropriate for approximately 20 m1.xlarge nodes,
  // in which each node has 15 GB of memory. Use this variable to scale the values (e.g.
  // number of records in a generated dataset) if you are running the tests with more
  // or fewer nodes. When developing new test suites, you might want to set this to a small
  // value suitable for a single machine, such as 0.001.
  val SCALE_FACTOR = 0.001
  assert(SCALE_FACTOR > 0, "SCALE_FACTOR must be > 0.")

  // If set, removes the first N trials for each test from all reported statistics. Useful for
  // tests which have outlier behavior due to JIT and other system cache warm-ups. If any test
  // returns fewer N + 1 results, an exception is thrown.
  val IGNORED_TRIALS = 0

  // Set up VariableOptions. Note that giant cross product is done over all JavaOptionsSets + VariableOptions
  // passed to each test which may be combinations of those set up here.

  // The following options value sets are shared among all tests.
  val COMMON_OPTS = Seq(
    // How many times to run each experiment - used to warm up system caches.
    // This VariableOption should probably only have a single value (i.e., length 1)
    // since it doesn't make sense to have multiple values here.
    VariableOption("num-trials", Seq(1)),
    // Extra pause added between trials, in seconds. For runs with large amounts
    // of shuffle data, this gives time for buffer cache write-back.
    VariableOption("inter-trial-wait", Seq(3))
  )

  // ================== //
  //  MLlib Test Setup  //
  // ================== //

  var MLLIB_TESTS = Seq[TestInstance]()
  val MLLIB_PERF_TEST_RUNNER = "mllib.perf.TestRunner"

  // Set this to 1.0, 1.1, 1.2, ... (the major version) to test MLlib with a particular Spark version.
  // Note: You should also build mllib-perf using -Dspark.version to specify the same version.
  // Note: To run perf tests against a snapshot version of Spark which has not yet been packaged into a release:
  //  * Build Spark locally by running `build/sbt assembly; build/sbt publishLocal` in the Spark root directory
  //  * Set `USE_CLUSTER_SPARK = True` and `MLLIB_SPARK_VERSION = {desired Spark version, e.g. 1.5}`
  //  * Don't use PREP_MLLIB_TESTS = True; instead manually run `cd mllib-tests; sbt/sbt -Dspark.version=1.5.0-SNAPSHOT clean assembly` to build perf tests
  val MLLIB_SPARK_VERSION = 1.5

  // The following options value sets are shared among all tests of
  // operations on MLlib algorithms.
  var MLLIB_COMMON_OPTS = COMMON_OPTS ++ Seq(
    // The number of input partitions.
    // The default setting is suitable for a 16-node m3.2xlarge EC2 cluster.
    VariableOption("num-partitions", Seq(128), can_scale = true),
    // A random seed to make tests reproducable.
    VariableOption("random-seed", Seq(5))
  )

  // Algorithms available in Spark-1.0 //

  // Regression and Classification Tests //
  val MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of rows or examples
    VariableOption("num-examples", Seq(1000000), can_scale = true),
    // The number of features per example
    VariableOption("num-features", Seq(10000), can_scale = false)
  )

  // Generalized Linear Model (GLM) Tests //
  var MLLIB_GLM_TEST_OPTS = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS ++ Seq(
    // The number of iterations for SGD
    VariableOption("num-iterations", Seq(20)),
    // The step size for SGD
    VariableOption("step-size", Seq(0.001)),
    // Regularization type: none, l1, l2
    VariableOption("reg-type", Seq("l2")),
    // Regularization parameter
    VariableOption("reg-param", Seq(0.1))
  )
  if (MLLIB_SPARK_VERSION >= 1.1) {
    MLLIB_GLM_TEST_OPTS ++= Seq(
      // Optimization algorithm: sgd, lbfgs
      VariableOption("optimizer", Seq("sgd"))
    )
  }

  // GLM Regression Tests //
  val MLLIB_GLM_REGRESSION_TEST_OPTS = MLLIB_GLM_TEST_OPTS ++ Seq(
    // The intercept for the data
    VariableOption("intercept", Seq(0.0)),
    // The scale factor for the noise
    VariableOption("epsilon", Seq(0.1)),
    // Loss to minimize: l2 (squared error)
    VariableOption("loss", Seq("l2"))
  )

  MLLIB_TESTS ++= Seq(TestInstance("glm-regression", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
    Seq(ConstantOption("glm-regression")) ++ MLLIB_GLM_REGRESSION_TEST_OPTS))

  // Classification Tests //
  val MLLIB_CLASSIFICATION_TEST_OPTS = MLLIB_GLM_TEST_OPTS ++ Seq(
    // Expected fraction of examples which are negative
    VariableOption("per-negative", Seq(0.3) ),
    // The scale factor for the noise in feature values
    VariableOption("scale-factor", Seq(1.0) )
  )

  // GLM Classification Tests //
  val MLLIB_GLM_CLASSIFICATION_TEST_OPTS = MLLIB_CLASSIFICATION_TEST_OPTS ++ Seq(
    // Loss to minimize: logistic, hinge (SVM)
    VariableOption("loss", Seq("logistic", "hinge") )
  )

  MLLIB_TESTS ++= Seq(TestInstance("glm-classification", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
    Seq(ConstantOption("glm-classification")) ++ MLLIB_GLM_CLASSIFICATION_TEST_OPTS))

  val NAIVE_BAYES_TEST_OPTS = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS ++ Seq(
    // Expected fraction of examples which are negative
    VariableOption("per-negative", Seq(0.3) ),
    // The scale factor for the noise in feature values
    VariableOption("scale-factor", Seq(1.0) ),
    // Naive Bayes smoothing lambda.
    VariableOption("nb-lambda", Seq(1.0) ),
    // Model type: either Multinomial or Bernoulli
    VariableOption("model-type", Seq("multinomial") )
  )

  MLLIB_TESTS ++= Seq(TestInstance("naive-bayes", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
    Seq(ConstantOption("naive-bayes")) ++ NAIVE_BAYES_TEST_OPTS))

  if (MLLIB_SPARK_VERSION >= 1.4) {
    val NAIVE_BAYES_TEST_OPTS_BERNOULLI = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS ++ Seq(
      // Expected fraction of examples which are negative
      VariableOption("per-negative", Seq(0.3) ),
      // The scale factor for the noise in feature values
      VariableOption("scale-factor", Seq(1.0) ),
      // Naive Bayes smoothing lambda.
      VariableOption("nb-lambda", Seq(1.0) ),
      // MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS + [
      VariableOption("model-type", Seq("bernoulli") )
    )
    MLLIB_TESTS ++= Seq(TestInstance("naive-bayes-bernoulli", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
      Seq(ConstantOption("naive-bayes")) ++ NAIVE_BAYES_TEST_OPTS_BERNOULLI))
  }

    // Decision Trees //
    var MLLIB_DECISION_TREE_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
      // The number of rows or examples
      VariableOption("num-examples", Seq(1000000), can_scale=true),
      // The number of features per example
      VariableOption("num-features", Seq(500), can_scale=false),
      // Type of label: 0 indicates regression, 2+ indicates classification with this many classes
      // Note: multi-class (>2) is not supported in Spark 1.0.
      VariableOption("label-type", Seq(0, 2), can_scale=false),
      // Fraction of features which are categorical
      VariableOption("frac-categorical-features", Seq(0.5), can_scale=false),
      // Fraction of categorical features which are binary. Others have 20 categories.
      VariableOption("frac-binary-features", Seq(0.5), can_scale=false),
      // Depth of true decision tree model used to label examples.
      // WARNING: The meaning of depth changed from Spark 1.0 to Spark 1.1:
      //          depth=N for Spark 1.0 should be depth=N-1 for Spark 1.1
      VariableOption("tree-depth", Seq(5, 10), can_scale=false),
      // Maximum number of bins for the decision tree learning algorithm.
      VariableOption("max-bins", Seq(32), can_scale=false)
    )

  var ensembleTypes = Seq[String]()
    if (MLLIB_SPARK_VERSION >= 1.2) {
      ensembleTypes ++= Seq("RandomForest")
    }
    if (MLLIB_SPARK_VERSION >= 1.3) {
      ensembleTypes ++= Seq("GradientBoostedTrees")
    }
    if (MLLIB_SPARK_VERSION >= 1.4) {
      ensembleTypes ++= Seq("ml.RandomForest", "ml.GradientBoostedTrees")
    }
    MLLIB_DECISION_TREE_TEST_OPTS ++= Seq(
      // Ensemble type: mllib.RandomForest, mllib.GradientBoostedTrees,
      //                ml.RandomForest, ml.GradientBoostedTrees
      VariableOption("ensemble-type", ensembleTypes),
      //Path to training dataset (if not given, use random data).
      VariableOption("training-data", Seq("")),
      //Path to test dataset (only used if training dataset given).
      //If not given, hold out part of training data for validation.
      VariableOption("test-data", Seq("")),
      //Fraction of data to hold out for testing (ignored if given training and test dataset).
      VariableOption("test-data-fraction", Seq(0.2), can_scale=false),
      //Number of trees. If 1, then run DecisionTree. If >1, then run RandomForest.
      VariableOption("num-trees", Seq(1, 10), can_scale=false),
      //Feature subset sampling strategy: auto, all, sqrt, log2, onethird
      //(only used for RandomForest)
      VariableOption("feature-subset-strategy", Seq("auto"))
    )

    MLLIB_TESTS = Seq(TestInstance("decision-tree", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
      Seq(ConstantOption("decision-tree")) ++ MLLIB_DECISION_TREE_TEST_OPTS))

  //  // Recommendation Tests //
  //  MLLIB_RECOMMENDATION_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  // The number of users
  //    VariableOption("num-users", [6000000], can_scale=True),
  //  // The number of products
  //    VariableOption("num-products", [5000000], can_scale=False),
  //  // The number of ratings
  //    VariableOption("num-ratings", [50000000], can_scale=True),
  //  // The number of iterations for ALS
  //  VariableOption("num-iterations", [10]),
  //  // The rank of the factorized matrix model
  //  VariableOption("rank", [10]),
  //  // The regularization parameter
  //  VariableOption("reg-param", [0.1]),
  //  // Whether to use implicit preferences or not
  //  FlagSet("implicit-prefs", [False])
  //  ]
  //
  //  MLLIB_TESTS += [("als", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("als")] +
  //    MLLIB_RECOMMENDATION_TEST_OPTS)]
  //
  //  // Clustering Tests //
  //  MLLIB_CLUSTERING_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  // The number of points
  //    VariableOption("num-points", [1000000], can_scale=True),
  //  // The number of features per point
  //    VariableOption("num-columns", [10000], can_scale=False),
  //  // The number of centers
  //    VariableOption("num-centers", [20]),
  //  // The number of iterations for KMeans
  //  VariableOption("num-iterations", [20])
  //  ]
  //
  //  MLLIB_TESTS += [("kmeans", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("kmeans")] + MLLIB_CLUSTERING_TEST_OPTS)]
  //
  //  MLLIB_GMM_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  VariableOption("num-points", [1000000], can_scale=True),
  //  VariableOption("num-columns", [100], can_scale=False),
  //  VariableOption("num-centers", [20], can_scale=False),
  //  VariableOption("num-iterations", [20])]
  //
  //  if MLLIB_SPARK_VERSION >= 1.3:
  //    MLLIB_TESTS += [("gmm", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("gmm")] + MLLIB_CLUSTERING_TEST_OPTS)]
  //
  //  MLLIB_LDA_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  VariableOption("num-documents", [10000], can_scale=True),
  //  VariableOption("num-vocab", [1000], can_scale=False),
  //  VariableOption("num-topics", [20], can_scale=False),
  //  VariableOption("num-iterations", [20]),
  //  VariableOption("document-length", [100])]
  //
  //  if MLLIB_SPARK_VERSION >= 1.4:
  //    MLLIB_TESTS += [("emlda", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("emlda")] + MLLIB_LDA_TEST_OPTS)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.4:
  //    MLLIB_TESTS += [("onlinelda", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("onlinelda")] + MLLIB_LDA_TEST_OPTS)]
  //
  //  // Linear Algebra Tests //
  //  MLLIB_LINALG_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  // The number of rows for the matrix
  //    VariableOption("num-rows", [1000000], can_scale=True),
  //  // The number of columns for the matrix
  //    VariableOption("num-cols", [1000], can_scale=False),
  //  // The number of top singular values wanted for SVD and PCA
  //  VariableOption("rank", [50], can_scale=False)
  //  ]
  //  // Linear Algebra Tests which take more time (slightly smaller settings) //
  //  MLLIB_BIG_LINALG_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  // The number of rows for the matrix
  //    VariableOption("num-rows", [1000000], can_scale=True),
  //  // The number of columns for the matrix
  //    VariableOption("num-cols", [500], can_scale=False),
  //  // The number of top singular values wanted for SVD and PCA
  //  VariableOption("rank", [10], can_scale=False)
  //  ]
  //
  //  MLLIB_TESTS += [("svd", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("svd")] + MLLIB_BIG_LINALG_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("pca", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("pca")] + MLLIB_LINALG_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("summary-statistics", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("summary-statistics")] +
  //    MLLIB_LINALG_TEST_OPTS)]
  //
  //  MLLIB_BLOCK_MATRIX_MULT_TEST_OPTS = MLLIB_COMMON_OPTS + [
  //  VariableOption("m", [20000], can_scale=True),
  //  VariableOption("k", [10000], can_scale=False),
  //  VariableOption("n", [10000], can_scale=False),
  //  VariableOption("block-size", [1024], can_scale=False)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.3:
  //    MLLIB_TESTS += [("block-matrix-mult", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("block-matrix-mult")] + MLLIB_BLOCK_MATRIX_MULT_TEST_OPTS)]
  //
  //  // Statistic Toolkit Tests //
  //  MLLIB_STATS_TEST_OPTS = MLLIB_COMMON_OPTS
  //
  //  MLLIB_PEARSON_TEST_OPTS = MLLIB_STATS_TEST_OPTS + \
  //    [VariableOption("num-rows", [1000000], can_scale=True),
  //  VariableOption("num-cols", [1000], can_scale=False)]
  //
  //  MLLIB_SPEARMAN_TEST_OPTS = MLLIB_STATS_TEST_OPTS + \
  //    [VariableOption("num-rows", [1000000], can_scale=True),
  //  VariableOption("num-cols", [100], can_scale=False)]
  //
  //  MLLIB_CHI_SQ_FEATURE_TEST_OPTS = MLLIB_STATS_TEST_OPTS + \
  //    [VariableOption("num-rows", [2000000], can_scale=True),
  //  VariableOption("num-cols", [500], can_scale=False)]
  //
  //  MLLIB_CHI_SQ_GOF_TEST_OPTS = MLLIB_STATS_TEST_OPTS + \
  //    [VariableOption("num-rows", [50000000], can_scale=True),
  //  VariableOption("num-cols", [0], can_scale=False)]
  //
  //  MLLIB_CHI_SQ_MAT_TEST_OPTS = MLLIB_STATS_TEST_OPTS + \
  //    [VariableOption("num-rows", [20000], can_scale=True),
  //  VariableOption("num-cols", [0], can_scale=False)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.1:
  //    MLLIB_TESTS += [("pearson", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("pearson")] + MLLIB_PEARSON_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("spearman", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("spearman")] + MLLIB_SPEARMAN_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("chi-sq-feature", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("chi-sq-feature")] + MLLIB_CHI_SQ_FEATURE_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("chi-sq-gof", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("chi-sq-gof")] + MLLIB_CHI_SQ_GOF_TEST_OPTS)]
  //
  //  MLLIB_TESTS += [("chi-sq-mat", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("chi-sq-mat")] + MLLIB_CHI_SQ_MAT_TEST_OPTS)]
  //
  //  // Feature Transformation Tests //
  //
  //  MLLIB_FEATURE_TEST_OPTS = MLLIB_COMMON_OPTS
  //
  //  MLLIB_WORD2VEC_TEST_OPTS = MLLIB_FEATURE_TEST_OPTS + \
  //    [VariableOption("num-sentences", [1000000], can_scale=True),
  //  VariableOption("num-words", [10000], can_scale=False),
  //  VariableOption("vector-size", [100], can_scale=False),
  //  VariableOption("num-iterations", [3], can_scale=False),
  //  VariableOption("min-count", [5], can_scale=False)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.3:  // TODO: make it work in 1.2
  //  MLLIB_TESTS += [("word2vec", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("word2vec")] + MLLIB_WORD2VEC_TEST_OPTS)]
  //
  //  // Frequent Pattern Matching Tests //
  //
  //  MLLIB_FPM_TEST_OPTS = MLLIB_COMMON_OPTS
  //
  //  MLLIB_FP_GROWTH_TEST_OPTS = MLLIB_FPM_TEST_OPTS + \
  //    [VariableOption("num-baskets", [5000000], can_scale=True),
  //  VariableOption("avg-basket-size", [10], can_scale=False),
  //  VariableOption("num-items", [1000], can_scale=False),
  //  VariableOption("min-support", [0.01], can_scale=False)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.3:
  //    MLLIB_TESTS += [("fp-growth", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("fp-growth")] + MLLIB_FP_GROWTH_TEST_OPTS)]
  //
  //  // TODO: tune test size to have runtime within 30-60 seconds
  //    MLLIB_PREFIX_SPAN_TEST_OPTS = MLLIB_FPM_TEST_OPTS + \
  //    [VariableOption("num-sequences", [5000000], can_scale=True),
  //  VariableOption("avg-sequence-size", [5], can_scale=False),
  //  VariableOption("avg-itemset-size", [1], can_scale=False),
  //  VariableOption("num-items", [100], can_scale=False),
  //  VariableOption("min-support", [0.5], can_scale=False),
  //  VariableOption("max-pattern-len", [10], can_scale=False),
  //  VariableOption("max-local-proj-db-size", [32000000], can_scale=False)]
  //
  //  if MLLIB_SPARK_VERSION >= 1.5:
  //    MLLIB_TESTS += [("prefix-span", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    MLLIB_JAVA_OPTS, [ConstantOption("prefix-span")] + MLLIB_PREFIX_SPAN_TEST_OPTS)]

  // ONLY RUN Prefix Span
  //  if (MLLIB_SPARK_VERSION >= 1.5):
  //    val MLLIB_TESTS = [("glm-regression", MLLIB_PERF_TEST_RUNNER, SCALE_FACTOR,
  //    val MLLIB_JAVA_OPTS, [ConstantOption("glm-regression")] +
  //    MLLIB_GLM_ELASTIC_NET_REGRESSION_TEST_OPTS)]
}
