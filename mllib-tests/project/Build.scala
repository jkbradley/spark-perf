import sbt._
import Keys._

object MyBuild extends Build {

  val deps = Seq(
    "net.sf.jopt-simple" % "jopt-simple" % "4.5",
    "org.scalatest" %% "scalatest" % "2.2.1" % "test",
    "org.slf4j" % "slf4j-log4j12" % "1.7.2"
  )

    lazy val root = project.in(file(".")).aggregate(onepointoh,onepointone)

    lazy val onepointone = project
        .settings(
          libraryDependencies ++= deps, 
          libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.1-SNAPSHOT" % "provided"
        )

    lazy val onepointoh = project
        .settings(        //should be set to 1.0.0 or higher
          libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.0.0" % "provided",
          libraryDependencies ++= deps 
        )
}
