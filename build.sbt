//
// http://spark.apache.org/docs/latest/quick-start.html#a-standalone-app-in-scala
//
name := """hello-apache-spark"""

// 2.11 doesn't seem to work
scalaVersion := "2.10.4"

libraryDependencies ++= Dependencies.sparkHadoop

libraryDependencies += "net.sf.opencsv" % "opencsv" % "2.3"

libraryDependencies += "org.apache.spark" % "spark-sql_2.10" % "1.3.1"


releaseSettings

scalariformSettings


fork in run := true