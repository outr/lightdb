name := "lightdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.4"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

fork := true

val haloDBVersion: String = "0.5.3"
val catsEffectVersion: String = "3.0.0-M5"
val profigVersion: String = "3.1.2"
val luceneVersion: String = "8.8.0"
val scribeVersion: String = "3.3.2"

libraryDependencies ++= Seq(
	"com.oath.halodb" % "halodb" % haloDBVersion,
	"org.typelevel" %% "cats-effect" % catsEffectVersion,
	"com.outr" %% "profig-all" % profigVersion,
	"org.apache.lucene" % "lucene-core" % luceneVersion,
	"com.outr" %% "scribe" % scribeVersion
)