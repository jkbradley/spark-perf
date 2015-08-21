package mllib.perf

/**
 * @param SCALE_FACTOR The default values configured below are appropriate for approximately 20 m1.xlarge nodes,
 *                     in which each node has 15 GB of memory. Use this variable to scale the values (e.g.
 *                     number of records in a generated dataset) if you are running the tests with more
 *                     or fewer nodes. When developing new test suites, you might want to set this to a small
 *                     value suitable for a single machine, such as 0.001.
 *
 */
class Config(SCALE_FACTOR: Double = 1.0, numTrials: Int = 1) {
  // ============================
  //  Test Configuration Options
  // ============================
  assert(SCALE_FACTOR > 0, "SCALE_FACTOR must be > 0.")

  // Set up VariableOptions. Note that giant cross product is done over all JavaOptionsSets + VariableOptions
  // passed to each test which may be combinations of those set up here.

  // The following options value sets are shared among all tests.
  var COMMON_OPTS = Seq(
    // How many times to run each experiment - used to warm up system caches.
    // This VariableOption should probably only have a single value (i.e., length 1)
    // since it doesn't make sense to have multiple values here.
    VariableOption("num-trials", Seq(numTrials)),
    // Extra pause added between trials, in seconds. For runs with large amounts
    // of shuffle data, this gives time for buffer cache write-back.
    VariableOption("inter-trial-wait", Seq(3))
  )

  // ================== //
  //  MLlib Test Setup  //
  // ================== //
  var MLLIB_TESTS = Seq[TestInstance]()

  // Set this to 1.0, 1.1, 1.2, ... (the major version) to test MLlib with a particular Spark version.
  // Note: You should also build mllib-perf using -Dspark.version to specify the same version.
  // Note: To run perf tests against a snapshot version of Spark which has not yet been packaged into a release:
  //  * Build Spark locally by running `build/sbt assembly; build/sbt publishLocal` in the Spark root directory
  //  * Set `USE_CLUSTER_SPARK = true` and `MLLIB_SPARK_VERSION = {desired Spark version, e.g. 1.5}`
  //  * Don't use PREP_MLLIB_TESTS = true; instead manually run `cd mllib-tests; sbt/sbt -Dspark.version=1.5.0-SNAPSHOT clean assembly` to build perf tests
  var MLLIB_SPARK_VERSION = 1.5

  // The following options value sets are shared among all tests of
  // operations on MLlib algorithms.
  var MLLIB_COMMON_OPTS = COMMON_OPTS ++ Seq(
    // The number of input partitions.
    // The default setting is suitable for a 16-node m3.2xlarge EC2 cluster.
    VariableOption("num-partitions", Seq(128), canScale = true),
    // A random seed to make tests reproducable.
    VariableOption("random-seed", Seq(5))
  )

  // Algorithms available in Spark-1.0 //

  // Regression and Classification Tests //
  val MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of rows or examples
    VariableOption("num-examples", Seq(1000000), canScale = true),
    // The number of features per example
    VariableOption("num-features", Seq(10000), canScale = false)
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

  MLLIB_TESTS ++= Seq(TestInstance("glm-regression", SCALE_FACTOR,
    Seq(ConstantOption("glm-regression")) ++ MLLIB_GLM_REGRESSION_TEST_OPTS))

  // Classification Tests //
  val MLLIB_CLASSIFICATION_TEST_OPTS = MLLIB_GLM_TEST_OPTS ++ Seq(
    // Expected fraction of examples which are negative
    VariableOption("per-negative", Seq(0.3)),
    // The scale factor for the noise in feature values
    VariableOption("scale-factor", Seq(1.0))
  )

  // GLM Classification Tests //
  val MLLIB_GLM_CLASSIFICATION_TEST_OPTS = MLLIB_CLASSIFICATION_TEST_OPTS ++ Seq(
    // Loss to minimize: logistic, hinge (SVM)
    VariableOption("loss", Seq("logistic", "hinge"))
  )

  MLLIB_TESTS ++= Seq(TestInstance("glm-classification", SCALE_FACTOR,
    Seq(ConstantOption("glm-classification")) ++ MLLIB_GLM_CLASSIFICATION_TEST_OPTS))

  val NAIVE_BAYES_TEST_OPTS = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS ++ Seq(
    // Expected fraction of examples which are negative
    VariableOption("per-negative", Seq(0.3)),
    // The scale factor for the noise in feature values
    VariableOption("scale-factor", Seq(1.0)),
    // Naive Bayes smoothing lambda.
    VariableOption("nb-lambda", Seq(1.0)),
    // Model type: either Multinomial or Bernoulli
    VariableOption("model-type", Seq("multinomial"))
  )

  MLLIB_TESTS ++= Seq(TestInstance("naive-bayes", SCALE_FACTOR,
    Seq(ConstantOption("naive-bayes")) ++ NAIVE_BAYES_TEST_OPTS))

  if (MLLIB_SPARK_VERSION >= 1.4) {
    val NAIVE_BAYES_TEST_OPTS_BERNOULLI = MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS ++ Seq(
      // Expected fraction of examples which are negative
      VariableOption("per-negative", Seq(0.3)),
      // The scale factor for the noise in feature values
      VariableOption("scale-factor", Seq(1.0)),
      // Naive Bayes smoothing lambda.
      VariableOption("nb-lambda", Seq(1.0)),
      // MLLIB_REGRESSION_CLASSIFICATION_TEST_OPTS + [
      VariableOption("model-type", Seq("bernoulli"))
    )
    MLLIB_TESTS ++= Seq(TestInstance("naive-bayes-bernoulli", SCALE_FACTOR,
      Seq(ConstantOption("naive-bayes")) ++ NAIVE_BAYES_TEST_OPTS_BERNOULLI))
  }

  // Decision Trees //
  var MLLIB_DECISION_TREE_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of rows or examples
    VariableOption("num-examples", Seq(1000000), canScale = true),
    // The number of features per example
    VariableOption("num-features", Seq(500), canScale = false),
    // Type of label: 0 indicates regression, 2+ indicates classification with this many classes
    // Note: multi-class (>2) is not supported in Spark 1.0.
    VariableOption("label-type", Seq(0, 2), canScale = false),
    // Fraction of features which are categorical
    VariableOption("frac-categorical-features", Seq(0.5), canScale = false),
    // Fraction of categorical features which are binary. Others have 20 categories.
    VariableOption("frac-binary-features", Seq(0.5), canScale = false),
    // Depth of true decision tree model used to label examples.
    // WARNING: The meaning of depth changed from Spark 1.0 to Spark 1.1:
    //          depth=N for Spark 1.0 should be depth=N-1 for Spark 1.1
    VariableOption("tree-depth", Seq(5, 10), canScale = false),
    // Maximum number of bins for the decision tree learning algorithm.
    VariableOption("max-bins", Seq(32), canScale = false)
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
    VariableOption("test-data-fraction", Seq(0.2), canScale = false),
    //Number of trees. If 1, then run DecisionTree. If >1, then run RandomForest.
    VariableOption("num-trees", Seq(1, 10), canScale = false),
    //Feature subset sampling strategy: auto, all, sqrt, log2, onethird
    //(only used for RandomForest)
    VariableOption("feature-subset-strategy", Seq("auto"))
  )

  MLLIB_TESTS ++= Seq(TestInstance("decision-tree", SCALE_FACTOR,
    Seq(ConstantOption("decision-tree")) ++ MLLIB_DECISION_TREE_TEST_OPTS))

  // Recommendation Tests //
  val MLLIB_RECOMMENDATION_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of users
    VariableOption("num-users", Seq(6000000), canScale = true),
    // The number of products
    VariableOption("num-products", Seq(5000000), canScale = false),
    // The number of ratings
    VariableOption("num-ratings", Seq(50000000), canScale = true),
    // The number of iterations for ALS
    VariableOption("num-iterations", Seq(10)),
    // The rank of the factorized matrix model
    VariableOption("rank", Seq(10)),
    // The regularization parameter
    VariableOption("reg-param", Seq(0.1)),
    // Whether to use implicit preferences or not
    FlagSet("implicit-prefs", Seq(false))
  )

  MLLIB_TESTS ++= Seq(TestInstance("als", SCALE_FACTOR,
    Seq(ConstantOption("als")) ++ MLLIB_RECOMMENDATION_TEST_OPTS))

  // Clustering Tests //
  val MLLIB_CLUSTERING_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of points
    VariableOption("num-points", Seq(1000000), canScale = true),
    // The number of features per point
    VariableOption("num-columns", Seq(2000), canScale = false),
    // The number of centers
    VariableOption("num-centers", Seq(20)),
    // The number of iterations for KMeans
    VariableOption("num-iterations", Seq(20))
  )

  MLLIB_TESTS ++= Seq(TestInstance("kmeans", SCALE_FACTOR,
    Seq(ConstantOption("kmeans")) ++ MLLIB_CLUSTERING_TEST_OPTS))

  val MLLIB_GMM_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    VariableOption("num-points", Seq(1000000), canScale = true),
    VariableOption("num-columns", Seq(100), canScale = false),
    VariableOption("num-centers", Seq(20), canScale = false),
    VariableOption("num-iterations", Seq(20)))

  if (MLLIB_SPARK_VERSION >= 1.3) {
    MLLIB_TESTS ++= Seq(TestInstance("gmm", SCALE_FACTOR,
      Seq(ConstantOption("gmm")) ++ MLLIB_GMM_TEST_OPTS))
  }

  val MLLIB_LDA_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    VariableOption("num-documents", Seq(10000), canScale = true),
    VariableOption("num-vocab", Seq(1000), canScale = false),
    VariableOption("num-topics", Seq(20), canScale = false),
    VariableOption("num-iterations", Seq(20)),
    VariableOption("document-length", Seq(100)))

  if (MLLIB_SPARK_VERSION >= 1.4) {
    MLLIB_TESTS ++= Seq(TestInstance("emlda", SCALE_FACTOR,
      Seq(ConstantOption("emlda")) ++ MLLIB_LDA_TEST_OPTS))
  }

  if (MLLIB_SPARK_VERSION >= 1.4) {
    MLLIB_TESTS ++= Seq(TestInstance("onlinelda", SCALE_FACTOR,
      Seq(ConstantOption("onlinelda")) ++ MLLIB_LDA_TEST_OPTS))
  }

  // Linear Algebra Tests //
  val MLLIB_LINALG_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of rows for the matrix
    VariableOption("num-rows", Seq(1000000), canScale = true),
    // The number of columns for the matrix
    VariableOption("num-cols", Seq(1000), canScale = false),
    // The number of top singular values wanted for SVD and PCA
    VariableOption("rank", Seq(50), canScale = false)
  )
  // Linear Algebra Tests which take more time (slightly smaller settings) //
  val MLLIB_BIG_LINALG_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    // The number of rows for the matrix
    VariableOption("num-rows", Seq(1000000), canScale = true),
    // The number of columns for the matrix
    VariableOption("num-cols", Seq(500), canScale = false),
    // The number of top singular values wanted for SVD and PCA
    VariableOption("rank", Seq(10), canScale = false)
  )

  MLLIB_TESTS ++= Seq(TestInstance("svd", SCALE_FACTOR,
    Seq(ConstantOption("svd")) ++ MLLIB_BIG_LINALG_TEST_OPTS))

  MLLIB_TESTS ++= Seq(TestInstance("pca", SCALE_FACTOR,
    Seq(ConstantOption("pca")) ++ MLLIB_LINALG_TEST_OPTS))

  MLLIB_TESTS ++= Seq(TestInstance("summary-statistics", SCALE_FACTOR,
    Seq(ConstantOption("summary-statistics")) ++
      MLLIB_LINALG_TEST_OPTS))

  val MLLIB_BLOCK_MATRIX_MULT_TEST_OPTS = MLLIB_COMMON_OPTS ++ Seq(
    VariableOption("m", Seq(20000), canScale = true),
    VariableOption("k", Seq(10000), canScale = false),
    VariableOption("n", Seq(10000), canScale = false),
    VariableOption("block-size", Seq(1024), canScale = false))

  if (MLLIB_SPARK_VERSION >= 1.3) {
    MLLIB_TESTS ++= Seq(TestInstance("block-matrix-mult", SCALE_FACTOR,
      Seq(ConstantOption("block-matrix-mult")) ++ MLLIB_BLOCK_MATRIX_MULT_TEST_OPTS))
  }

  // Statistic Toolkit Tests //
  val MLLIB_STATS_TEST_OPTS = MLLIB_COMMON_OPTS

  val MLLIB_PEARSON_TEST_OPTS = MLLIB_STATS_TEST_OPTS ++
    Seq(VariableOption("num-rows", Seq(1000000), canScale = true),
      VariableOption("num-cols", Seq(1000), canScale = false))

  val MLLIB_SPEARMAN_TEST_OPTS = MLLIB_STATS_TEST_OPTS ++
    Seq(VariableOption("num-rows", Seq(1000000), canScale = true),
      VariableOption("num-cols", Seq(100), canScale = false))

  val MLLIB_CHI_SQ_FEATURE_TEST_OPTS = MLLIB_STATS_TEST_OPTS ++
    Seq(VariableOption("num-rows", Seq(2000000), canScale = true),
      VariableOption("num-cols", Seq(500), canScale = false))

  val MLLIB_CHI_SQ_GOF_TEST_OPTS = MLLIB_STATS_TEST_OPTS ++
    Seq(VariableOption("num-rows", Seq(50000000), canScale = true),
      VariableOption("num-cols", Seq(0), canScale = false))

  val MLLIB_CHI_SQ_MAT_TEST_OPTS = MLLIB_STATS_TEST_OPTS ++
    Seq(VariableOption("num-rows", Seq(20000), canScale = true),
      VariableOption("num-cols", Seq(0), canScale = false))

  if (MLLIB_SPARK_VERSION >= 1.1) {
    MLLIB_TESTS ++= Seq(TestInstance("pearson", SCALE_FACTOR,
      Seq(ConstantOption("pearson")) ++ MLLIB_PEARSON_TEST_OPTS))

    MLLIB_TESTS ++= Seq(TestInstance("spearman", SCALE_FACTOR,
      Seq(ConstantOption("spearman")) ++ MLLIB_SPEARMAN_TEST_OPTS))

    MLLIB_TESTS ++= Seq(TestInstance("chi-sq-feature", SCALE_FACTOR,
      Seq(ConstantOption("chi-sq-feature")) ++ MLLIB_CHI_SQ_FEATURE_TEST_OPTS))

    MLLIB_TESTS ++= Seq(TestInstance("chi-sq-gof", SCALE_FACTOR,
      Seq(ConstantOption("chi-sq-gof")) ++ MLLIB_CHI_SQ_GOF_TEST_OPTS))

    MLLIB_TESTS ++= Seq(TestInstance("chi-sq-mat", SCALE_FACTOR,
      Seq(ConstantOption("chi-sq-mat")) ++ MLLIB_CHI_SQ_MAT_TEST_OPTS))
  }

  // Feature Transformation Tests //

  val MLLIB_FEATURE_TEST_OPTS = MLLIB_COMMON_OPTS

  val MLLIB_WORD2VEC_TEST_OPTS = MLLIB_FEATURE_TEST_OPTS ++
    Seq(VariableOption("num-sentences", Seq(1000000), canScale = true),
      VariableOption("num-words", Seq(10000), canScale = false),
      VariableOption("vector-size", Seq(100), canScale = false),
      VariableOption("num-iterations", Seq(3), canScale = false),
      VariableOption("min-count", Seq(5), canScale = false))

  if (MLLIB_SPARK_VERSION >= 1.3) {
    // TODO: make it work in 1.2
    MLLIB_TESTS ++= Seq(TestInstance("word2vec", SCALE_FACTOR,
      Seq(ConstantOption("word2vec")) ++ MLLIB_WORD2VEC_TEST_OPTS))
  }

  // Frequent Pattern Matching Tests //

  val MLLIB_FPM_TEST_OPTS = MLLIB_COMMON_OPTS

  val MLLIB_FP_GROWTH_TEST_OPTS = MLLIB_FPM_TEST_OPTS ++
    Seq(VariableOption("num-baskets", Seq(5000000), canScale = true),
      VariableOption("avg-basket-size", Seq(10), canScale = false),
      VariableOption("num-items", Seq(1000), canScale = false),
      VariableOption("min-support", Seq(0.01), canScale = false))

  if (MLLIB_SPARK_VERSION >= 1.3) {
    MLLIB_TESTS ++= Seq(TestInstance("fp-growth", SCALE_FACTOR,
      Seq(ConstantOption("fp-growth")) ++ MLLIB_FP_GROWTH_TEST_OPTS))
  }
  // TODO: tune test size to have runtime within 30-60 seconds
  val MLLIB_PREFIX_SPAN_TEST_OPTS = MLLIB_FPM_TEST_OPTS ++ Seq(
    VariableOption("num-sequences", Seq(5000000), canScale = false),
    VariableOption("avg-sequence-size", Seq(10), canScale = false),
    VariableOption("avg-itemset-size", Seq(1), canScale = false),
    VariableOption("num-items", Seq(100), canScale = false),
    VariableOption("min-support", Seq(0.1), canScale = false),
    VariableOption("max-pattern-len", Seq(10), canScale = false),
    VariableOption("max-local-proj-db-size", Seq(32000000), canScale = false))

  if (MLLIB_SPARK_VERSION >= 1.5) {
    MLLIB_TESTS ++= Seq(TestInstance("prefix-span", SCALE_FACTOR,
      Seq(ConstantOption("prefix-span")) ++ MLLIB_PREFIX_SPAN_TEST_OPTS))
  }
}

