package mllib.perf

import org.apache.spark.SparkContext

import scala.collection.JavaConverters._

import org.json4s.JsonDSL._
import org.json4s.JsonAST._

import mllib.perf.clustering.GaussianMixtureTest
import mllib.perf.feature.Word2VecTest
import mllib.perf.fpm.FPGrowthTest
import mllib.perf.linalg.BlockMatrixMultTest

object TestRunner {
  def run(sc: SparkContext, testName: String, perfTestArgs: Array[String]): JValue = {
    val test: PerfTest = testName match {
      case "glm-regression" => new GLMRegressionTest(sc)
      case "glm-classification" => new GLMClassificationTest(sc)
      case "naive-bayes" => new NaiveBayesTest(sc)
      // recommendation
      case "als" => new ALSTest(sc)
      // clustering
      case "kmeans" => new KMeansTest(sc)
      // trees
      case "decision-tree" => new DecisionTreeTest(sc)
      // linalg
      case "svd" => new SVDTest(sc)
      case "pca" => new PCATest(sc)
      // stats
      case "summary-statistics" => new ColumnSummaryStatisticsTest(sc)
      case "pearson" => new PearsonCorrelationTest(sc)
      case "spearman" => new SpearmanCorrelationTest(sc)
      case "chi-sq-feature" => new ChiSquaredFeatureTest(sc)
      case "chi-sq-gof" => new ChiSquaredGoFTest(sc)
      case "chi-sq-mat" => new ChiSquaredMatTest(sc)
      case "fp-growth" => new FPGrowthTest(sc)
      case "block-matrix-mult" => new BlockMatrixMultTest(sc)
      case "word2vec" => new Word2VecTest(sc)
      case "gmm" => new GaussianMixtureTest(sc)
    }
    test.initialize(testName, perfTestArgs)

    // Generate a new dataset for each test
    val rand = new java.util.Random(test.getRandomSeed)

    val numTrials = test.getNumTrials
    val interTrialWait = test.getWait

    var testOptions: JValue = test.getOptions
    val results: Seq[JValue] = (1 to numTrials).map { i =>
      test.createInputData(rand.nextLong())
      val res: JValue = test.run()
      System.gc()
      Thread.sleep(interTrialWait)
      res
    }

    val json: JValue =
      ("testName" -> testName) ~
        ("options" -> testOptions) ~
        ("sparkConf" -> sc.getConf.getAll.toMap) ~
        ("sparkVersion" -> sc.version) ~
        ("systemProperties" -> System.getProperties.asScala.toMap) ~
        ("results" -> results)
    json
  }
}
