name := "lightdb"
organization := "com.outr"
version := "1.0.0-SNAPSHOT"

scalaVersion := "2.13.5"

resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

fork := true

val haloDBVersion: String = "0.5.3"
val catsEffectVersion: String = "3.0.1"
val fabricVersion: String = "1.0.2"
val lucene4sVersion: String = "1.10.0"
val scribeVersion: String = "3.5.1"

val testyVersion: String = "1.0.2-SNAPSHOT"

Test / parallelExecution := false

libraryDependencies ++= Seq(
	"com.oath.halodb" % "halodb" % haloDBVersion,
	"org.typelevel" %% "cats-effect" % catsEffectVersion,
	"com.outr" %% "fabric-parse" % fabricVersion,
	"com.outr" %% "lucene4s" % lucene4sVersion,
	"com.outr" %% "scribe" % scribeVersion,

	"com.outr" %% "testy" % testyVersion % Test
)

testFrameworks += new TestFramework("munit.Framework")