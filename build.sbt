name := "AdAnalysis"

version := "0.1"

scalaVersion := "2.13.8"

libraryDependencies += "com.fasterxml.jackson.core" % "jackson-databind" % "2.13.1"
libraryDependencies += "com.fasterxml.jackson.module" %% "jackson-module-scala" % "2.13.1"
libraryDependencies += "org.apache.spark" %% "spark-core" % "3.2.0"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "3.2.0"