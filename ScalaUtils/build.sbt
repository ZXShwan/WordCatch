
name := "BulkLoad2"

version := "0.1"

scalaVersion := "2.10.5"

resolvers += "cloudera" at "https://repository.cloudera.com/artifactory/cloudera-repos/"

libraryDependencies += "org.apache.spark" % "spark-core_2.10" % "1.6.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-common" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-server" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-client" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.apache.hbase" % "hbase-hadoop-compat" % "1.2.0-cdh5.11.1"
libraryDependencies += "org.xerial.snappy" % "snappy-java" % "1.1.2.1"

assemblyOption in assembly := (assemblyOption in assembly).value.copy(includeScala = false, includeDependency = false)
