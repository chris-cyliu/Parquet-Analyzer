name := "ParquetTableAnalyzer"

version := "1.0"

scalaVersion := "2.11.8"

libraryDependencies += "org.apache.spark" % "spark-core_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-sql_2.11" % "2.0.0"

libraryDependencies += "org.apache.spark" % "spark-catalyst_2.11" % "2.0.0"

libraryDependencies += "org.apache.parquet" % "parquet-hadoop" % "1.8.3-SNAPSHOT-POLYU"

libraryDependencies += "org.apache.parquet" % "parquet-column" % "1.8.3-SNAPSHOT-POLYU"