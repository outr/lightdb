name := "testdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.3"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

libraryDependencies ++= Seq(
	"com.outr" %% "lucene4s" % "1.10.0",
	"io.youi" %% "youi-core" % "0.13.16",
	"com.oath.halodb" % "halodb" % "0.5.3"
)
