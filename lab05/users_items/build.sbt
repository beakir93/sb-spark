name := "users_items"

version := "1.0"

scalaVersion := "2.11.12"

libraryDependencies += "org.scalactic" %% "scalactic" % "3.1.1"

libraryDependencies += "org.scalatest" %% "scalatest" % "3.1.1" % "test"

//https://mvnrepository.com/artifact/org.apache.spark/spark-core
libraryDependencies ++= Seq(
  "org.apache.spark" %% "spark-sql" % "2.4.5",
  "org.apache.spark" %% "spark-core" % "2.4.5",
  "org.apache.spark" %% "spark-sql-kafka-0-10" % "2.4.5" % "provided"
)