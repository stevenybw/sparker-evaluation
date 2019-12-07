name := "SparkEvaluation"

version := "0.1"

scalaVersion := "2.11.12"

unmanagedJars := (baseDirectory.value ** "spark-*_2.11-2.3.0.jar").classpath
libraryDependencies += "org.zeromq" % "jeromq" % "0.5.1"
// libraryDependencies += "org.apache.spark" %% "spark-core" % "2.3.0"
// libraryDependencies += "org.apache.spark" %% "spark-mllib" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-streaming" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-graphx" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-mllib-local" % "2.3.0"
libraryDependencies += "org.apache.spark" %% "spark-tags" % "2.3.0"
libraryDependencies += "com.github.scopt" % "scopt_2.11" % "4.0.0-RC2"
