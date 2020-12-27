name := "testdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.4"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

libraryDependencies ++= Seq(
	"com.outr" %% "scribe" % "3.1.8",
	"io.youi" %% "youi-core" % "0.13.20",
	"com.oath.halodb" % "halodb" % "0.5.3"
)