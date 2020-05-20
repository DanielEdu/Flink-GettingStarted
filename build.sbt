name := "demo-project"

version := "0.1"

scalaVersion := "2.12.11"


// https://mvnrepository.com/artifact/org.apache.flink/flink-scala
libraryDependencies += "org.apache.flink" %% "flink-scala" % "1.9.3"
libraryDependencies += "org.apache.flink" % "flink-core" % "1.9.3"
libraryDependencies += "org.apache.flink" % "flink-table" % "1.9.2"
libraryDependencies += "org.apache.flink" %% "flink-streaming-scala" % "1.9.3"
libraryDependencies += "org.apache.flink" %% "flink-connector-kafka" % "1.9.3"

libraryDependencies += "org.apache.flink" %% "flink-table-planner-blink" % "1.9.3"
libraryDependencies += "org.apache.flink" %% "flink-table-planner" % "1.9.3"
libraryDependencies += "org.apache.flink" % "flink-table-common" % "1.9.3" % "provided"
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala" % "1.9.3"
libraryDependencies += "org.apache.flink" %% "flink-table-api-scala-bridge" % "1.9.3"
libraryDependencies += "org.apache.flink" %% "flink-table-api-java-bridge" % "1.9.3" % "provided"

