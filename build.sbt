name := "lightdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.5"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

fork := true

val haloDBVersion: String = "0.5.3"
val catsEffectVersion: String = "3.0.1"
val fabricVersion: String = "1.0.2"
val luceneVersion: String = "8.8.1"
val scribeVersion: String = "3.5.1"

val testyVersion: String = "1.0.2-SNAPSHOT"

Test / parallelExecution := false

libraryDependencies ++= Seq(
	"com.oath.halodb" % "halodb" % haloDBVersion,
	"org.typelevel" %% "cats-effect" % catsEffectVersion,
	"com.outr" %% "fabric-parse" % fabricVersion,
	"org.apache.lucene" % "lucene-core" % luceneVersion,
	"org.apache.lucene" % "lucene-analyzers-common" % luceneVersion,
	"org.apache.lucene" % "lucene-queryparser" % luceneVersion,
	"org.apache.lucene" % "lucene-facet" % luceneVersion,
	"org.apache.lucene" % "lucene-highlighter" % luceneVersion,
	"com.outr" %% "scribe" % scribeVersion,

	"com.outr" %% "testy" % testyVersion % Test
)

testFrameworks += new TestFramework("munit.Framework")