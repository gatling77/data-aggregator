name := "dataggregator"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies ++= {
  val sparkVer = "2.3.0"
  Seq(
    "org.apache.spark" %% "spark-core" % sparkVer
  )
}

libraryDependencies += "junit" % "junit" % "4.10" % "test"
libraryDependencies += "org.scalatest" % "scalatest_2.11" % "3.0.5" % "test"