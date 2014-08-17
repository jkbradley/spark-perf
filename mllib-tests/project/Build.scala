import sbt._
import Keys._

object MyBuild extends Build {

    lazy val root = project.in(file(".")).aggregate(onepointoh,onepointone).
      settings(
        libraryDependencies ++= Seq(
          "net.sf.jopt-simple" % "jopt-simple" % "4.5",
          "org.scalatest" %% "scalatest" % "2.2.1" % "test",
          "org.apache.spark" %% "spark-mllib" % "1.0.0" % "provided",
          "org.slf4j" % "slf4j-log4j12" % "1.7.2"
        )
      ).dependsOn(onepointoh, onepointone)

    lazy val onepointone = project
        .settings(
          libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5",
          libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.1.1-SNAPSHOT" % "provided"
        ).dependsOn(onepointoh)

    lazy val onepointoh = project
          .settings(        //should be set to 1.0.0 or higher
            libraryDependencies += "org.apache.spark" %% "spark-mllib" % "1.0.0" % "provided",
            libraryDependencies += "net.sf.jopt-simple" % "jopt-simple" % "4.5" 
          )
}
