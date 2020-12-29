name := "testdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "3.0.0-M3"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

libraryDependencies ++= Seq(
	"com.oath.halodb" % "halodb" % "0.5.3"
)