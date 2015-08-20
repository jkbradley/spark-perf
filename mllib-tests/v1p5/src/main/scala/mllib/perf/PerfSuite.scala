package mllib.perf


import org.apache.spark.SparkContext


sealed trait TestOption {
  def to_array(scale_factor: Double): Seq[String]
}

case class VariableOption(name: String, vals: Seq[Any], can_scale: Boolean = false) extends TestOption {
  def scaled_vals(scale_factor: Double): Seq[Any] = {
    if (can_scale) vals.map(v => math.max(1, (v.toString.toDouble * scale_factor).toInt))
    else vals
  }

  override def to_array(scale_factor: Double = 1.0): Seq[String] = {
    scaled_vals(scale_factor).map(v => s"--${this.name}=${v}")
  }
}

case class ConstantOption(name: String) extends TestOption {
  override def to_array(scale_factor: Double = 1.0): Seq[String] = Seq(this.name)
}

case class FlagSet(name: String, vals: Seq[Boolean]) extends TestOption {
  override def to_array(scale_factor: Double = 1.0): Seq[String] = vals.map { b =>
    if (b) s"--${this.name}"
    else ""
  }
}

case class TestInstance(shortName: String, mainClass: String, scaleFactor: Double, options: Seq[TestOption])

/**
 * Runs all the performance tests specified in [[Config]].
 */
object PerfSuite {
  val OUTPUT_DIVIDER_STRING = "-" * 68

  // TODO: Return results in a dataframe
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
