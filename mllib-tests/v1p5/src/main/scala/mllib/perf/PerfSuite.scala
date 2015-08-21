package mllib.perf


import org.json4s.jackson.JsonMethods._
import org.apache.spark.sql.{DataFrame, SQLContext}
import org.json4s.JsonAST.JValue


sealed trait TestOption {
  def toArray(scaleFactor: Double): Seq[String]
}

case class VariableOption(name: String, vals: Seq[Any], canScale: Boolean = false) extends TestOption {
  def scaledVals(scaleFactor: Double): Seq[Any] = {
    if (canScale) vals.map(v => math.max(1, (v.toString.toDouble * scaleFactor).toInt))
    else vals
  }

  override def toArray(scaleFactor: Double = 1.0): Seq[String] = {
    scaledVals(scaleFactor).map(v => s"--${this.name}=${v}")
  }
}

case class ConstantOption(name: String) extends TestOption {
  override def toArray(scaleFactor: Double = 1.0): Seq[String] = Seq(this.name)
}

case class FlagSet(name: String, vals: Seq[Boolean]) extends TestOption {
  override def toArray(scaleFactor: Double = 1.0): Seq[String] = vals.map { b =>
    if (b) s"--${this.name}"
    else ""
  }
}

case class TestInstance(shortName: String, mainClass: String, scaleFactor: Double, options: Seq[TestOption])

/**
 * The entry-point into [[mllib.perf]].
 */
object PerfSuite {
  val OUTPUT_DIVIDER_STRING = "-" * 68

  /**
   * Runs MLlib performance tests and writes results to `mountName`
   * @param sqlContext SQLContext to run test in
   * @param mountName Path to location for storing test results
   */
  def runSuite(sqlContext: SQLContext, config: Config, mountName: String): (Seq[String], DataFrame) = {
    val sc = sqlContext.sparkContext
    var failedTests = Seq[String]()
    val testResults = (for {
      TestInstance(shortName, _, scaleFactor, optSets) <- config.MLLIB_TESTS;
      optSetArrays = optSets.map(i => i.toArray(scaleFactor));
      optList <- optSetArrays.reduceLeft((xs, ys) => for {x <- xs; y <- ys} yield x + " " ++ y)
    } yield {
        val args = optList.split(" ")
        val testName = args(0)
        val perfTestArgs = args.slice(1, args.length)
        try {
          val results = TestRunner.run(sc, testName, perfTestArgs)
          val df = sqlContext.read.json(sc.parallelize(Seq(compact(results))))
          val timestamp = System.currentTimeMillis()
          df.write.format("json").save(s"$mountName/$testName-$timestamp")
          Some(results)
        } catch { case (e: Exception) =>
          failedTests ++= Seq(shortName)
          None
        }
      }).flatMap((x: Option[JValue]) => x)
    (failedTests, sqlContext.read.json(sc.parallelize(testResults.map(compact(_)))))
  }
}
