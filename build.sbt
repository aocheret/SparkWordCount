ThisBuild / version := "0.1.0-SNAPSHOT"

ThisBuild / scalaVersion := "2.12.1"

// https://mvnrepository.com/artifact/org.apache.hadoop/hadoop-client
libraryDependencies += "org.apache.hadoop" % "hadoop-client" % "3.4.0"
// https://mvnrepository.com/artifact/commons-io/commons-io
libraryDependencies += "commons-io" % "commons-io" % "2.16.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.5.1"
// https://mvnrepository.com/artifact/org.apache.spark/spark-sql
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.5.0"
// https://mvnrepository.com/artifact/org.apache.spark/spark-streaming
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "3.5.0"

lazy val root = (project in file("."))
  .settings(
    name := "Spark",
    idePackagePrefix := Some("org.itc.com")
  )
