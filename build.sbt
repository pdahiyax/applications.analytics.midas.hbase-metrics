
name := "MetricCapture"
version := "1.0"
scalaVersion := "2.11.12"



//scalacOptions += "-Ylog-classpath"

resolvers += "sbt-pack repo" at "https://repo1.maven.org/maven2/org/xerial/sbt/"
resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"
resolvers += "Spark Packages" at "https://dl.bintray.com/spark-packages/maven/"
resolvers += "MnvRepository" at "https://mvnrepository.com/artifact/"


val sparkVersion = "2.4.0.7.1.6.0-297"
val hbaseVersion = "2.2.3.7.1.6.0-297"
val hbaseConnectorsVersion = "1.0.0.7.1.6.0-297"
val hadoopVersion = "3.1.1.7.1.6.0-297"

libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-yarn" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-core" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-sql" % sparkVersion % Provided,
  "org.apache.spark" %% "spark-streaming" % sparkVersion % Provided excludeAll(
    ExclusionRule(organization = "com.fasterxml.jackson.core"),
    ExclusionRule(organization = "com.fasterxml.jackson.module")
  ),
  "org.apache.hbase" % "hbase-client" % hbaseVersion % Provided  ,
  "org.apache.hbase" % "hbase-common" % hbaseVersion % Provided  ,
  "org.apache.hbase" % "hbase-hadoop-compat" % hbaseVersion % Provided ,
  "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion % Provided  ,
  "org.apache.hbase" % "hbase-mapreduce" % hbaseVersion % Provided  ,
  "org.apache.hbase.connectors.spark" % "hbase-spark" % hbaseConnectorsVersion % Provided  ,
  "org.apache.hbase" % "hbase-server" % hbaseVersion % Provided excludeAll( ExclusionRule(organization = "org.mortbay.jetty")),
  "org.apache.hadoop" % "hadoop-client" % hadoopVersion % Provided  ,
  "org.apache.hadoop" % "hadoop-common" % hadoopVersion % Provided  excludeAll (
    ExclusionRule(organization = "javax.servlet"),
    ),
  "org.scalatest" %% "scalatest" % "3.0.4" % Test,
  "org.apache.hbase" % "hbase-testing-util" % hbaseVersion % Test,
  "org.scala-lang.modules" %% "scala-parser-combinators" % "1.1.2"

)



