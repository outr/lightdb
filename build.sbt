name := "lightdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.4"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

val haloDBVersion: String = "0.5.3"

libraryDependencies ++= Seq(
	"com.oath.halodb" % "halodb" % haloDBVersion
)