// Scala versions
val scala213 = "2.13.14"
val scala3 = "3.4.2"
val scala2 = List(scala213)
val allScalaVersions = scala3 :: scala2

// Variables
val org: String = "com.outr"
val projectName: String = "lightdb"
val githubOrg: String = "outr"
val email: String = "matt@matthicks.com"
val developerId: String = "darkfrog"
val developerName: String = "Matt Hicks"
val developerURL: String = "https://matthicks.com"

name := projectName
ThisBuild / organization := org
ThisBuild / version := "0.12.0-SNAPSHOT"
ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := allScalaVersions
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / sonatypeCredentialHost := "s01.oss.sonatype.org"
ThisBuild / sonatypeRepository := "https://s01.oss.sonatype.org/service/local"
ThisBuild / publishTo := sonatypePublishToBundle.value
ThisBuild / sonatypeProfileName := org
ThisBuild / licenses := Seq("MIT" -> url(s"https://github.com/$githubOrg/$projectName/blob/master/LICENSE"))
ThisBuild / sonatypeProjectHosting := Some(xerial.sbt.Sonatype.GitHubHosting(githubOrg, projectName, email))
ThisBuild / homepage := Some(url(s"https://github.com/$githubOrg/$projectName"))
ThisBuild / scmInfo := Some(
	ScmInfo(
		url(s"https://github.com/$githubOrg/$projectName"),
		s"scm:git@github.com:$githubOrg/$projectName.git"
	)
)
ThisBuild / developers := List(
	Developer(id=developerId, name=developerName, email=email, url=url(developerURL))
)

ThisBuild / resolvers += Resolver.mavenLocal
ThisBuild / resolvers += "jitpack" at "https://jitpack.io"

ThisBuild / outputStrategy := Some(StdoutOutput)

ThisBuild / javaOptions ++= Seq(
	"--enable-native-access=ALL-UNNAMED",
	"--add-modules", "jdk.incubator.vector"
)

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")

val collectionCompatVersion: String = "2.12.0"

val haloDBVersion: String = "0.5.6"

val rocksDBVersion: String = "9.2.1"

val catsEffectVersion: String = "3.5.4"

val fabricVersion: String = "1.15.1"

val fs2Version: String = "3.10.2"

val scribeVersion: String = "3.15.0"

val luceneVersion: String = "9.11.0"

val sqliteVersion: String = "3.46.0.0"

val duckdbVersion: String = "1.0.0"

val keysemaphoreVersion: String = "0.3.0-M1"

val squantsVersion: String = "1.8.3"

val scalaTestVersion: String = "3.2.18"

val catsEffectTestingVersion: String = "1.5.0"

lazy val root = project.in(file("."))
	.aggregate(core.jvm, core.js, halodb, rocksdb, mapdb, lucene, sql, sqlite, duckdb, all)
	.settings(
		name := projectName,
		publish := {},
		publishLocal := {}
	)

lazy val core = crossProject(JSPlatform, JVMPlatform) // TODO: Add when cats-effect supports native, NativePlatform)
	.crossType(CrossType.Pure)
	.settings(
		name := s"$projectName-core",
		libraryDependencies ++= Seq(
			"com.outr" %%% "scribe" % scribeVersion,
			"com.outr" %%% "scribe-cats" % scribeVersion,
			"org.typelevel" %%% "cats-effect" % catsEffectVersion,
			"org.typelevel" %%% "fabric-io" % fabricVersion,
			"co.fs2" %%% "fs2-core" % fs2Version,
			"org.typelevel" %%% "squants" % squantsVersion,
			"com.outr" %% "scribe-slf4j" % scribeVersion,
			"org.scalatest" %%% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %%% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		),
		libraryDependencies ++= (
			if (scalaVersion.value.startsWith("2.")) {
				Seq(
					"org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatVersion,
					"org.scala-lang" % "scala-reflect" % scalaVersion.value
				)
			} else {
				Nil
			}
		),
		Compile / unmanagedSourceDirectories ++= {
			val major = if (scalaVersion.value.startsWith("2.")) "-2" else "-3"
			List(CrossType.Pure, CrossType.Full).flatMap(
				_.sharedSrcDir(baseDirectory.value, "main").toList.map(f => file(f.getPath + major))
			)
		}
	)
	.jvmSettings(
		fork := true
	)

lazy val halodb = project.in(file("halodb"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-halo",
		fork := true,
		libraryDependencies ++= Seq(
			"com.github.yahoo" % "HaloDB" % haloDBVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val rocksdb = project.in(file("rocksdb"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-rocks",
		fork := true,
		libraryDependencies ++= Seq(
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val mapdb = project.in(file("mapdb"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-mapdb",
		libraryDependencies ++= Seq(
			"org.mapdb" % "mapdb" % "3.1.0",
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		),
		fork := true
	)

lazy val lucene = project.in(file("lucene"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-lucene",
		fork := true,
		libraryDependencies ++= Seq(
			"org.apache.lucene" % "lucene-core" % luceneVersion,
			"org.apache.lucene" % "lucene-queryparser" % luceneVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val sql = project.in(file("sql"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-sql",
		fork := true,
		libraryDependencies ++= Seq(
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val sqlite = project.in(file("sqlite"))
	.dependsOn(sql)
	.settings(
		name := s"$projectName-sqlite",
		fork := true,
		libraryDependencies ++= Seq(
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val duckdb = project.in(file("duckdb"))
	.dependsOn(sql)
	.settings(
		name := s"$projectName-duckdb",
		fork := true,
		libraryDependencies ++= Seq(
			"org.duckdb" % "duckdb_jdbc" % duckdbVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val all = project.in(file("all"))
	.dependsOn(core.jvm, halodb, rocksdb, mapdb, lucene, sqlite, duckdb)
	.settings(
		name := s"$projectName-all",
		fork := true,
		libraryDependencies ++= Seq(
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test,
			"org.typelevel" %% "cats-effect-testing-scalatest" % catsEffectTestingVersion % Test
		)
	)

lazy val benchmark = project.in(file("benchmark"))
	.dependsOn(all)
	.settings(
		name := s"$projectName-benchmark",
		fork := true,
		libraryDependencies ++= Seq(
			"co.fs2" %% "fs2-io" % fs2Version,
			"org.mongodb" % "mongodb-driver-sync" % "5.0.1",
			"org.postgresql" % "postgresql" % "42.7.3",
			"org.mariadb.jdbc" % "mariadb-java-client" % "3.3.3",
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"com.outr" %% "scarango-driver" % "3.20.0"
		)
	)