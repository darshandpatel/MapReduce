name := "PageRankSpark"

version := "1.0"

scalaVersion := "2.11.8"

fork in run := true

libraryDependencies += ("org.apache.spark" %% "spark-core" % "2.0.0")
libraryDependencies += "org.apache.commons" % "commons-compress" % "1.12"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql_2.11
