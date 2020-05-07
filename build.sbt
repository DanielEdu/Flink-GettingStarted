name := "demo-project"

version := "0.1"

scalaVersion := "2.11.12"


// https://mvnrepository.com/artifact/org.apache.flink/flink-scala
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.2"
libraryDependencies += "org.apache.flink" % "flink-core" % "1.9.2"
libraryDependencies += "org.apache.flink" % "flink-table" % "1.9.2" % "provided" pomOnly()
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.2"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.9.2"


