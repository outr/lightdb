// Scala versions
val scala213 = "2.13.14"
val scala3 = "3.5.0"
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
ThisBuild / version := "0.13.0-SNAPSHOT"
ThisBuild / scalaVersion := scala213
ThisBuild / crossScalaVersions := allScalaVersions
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")

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

ThisBuild / Test / testOptions += Tests.Argument("-n", "spec.EmbeddedTest")

val collectionCompatVersion: String = "2.12.0"

val reactifyVersion: String = "4.1.2"

val haloDBVersion: String = "0.5.6"

val rocksDBVersion: String = "9.5.2"

val mapdbVersion: String = "3.1.0"

val jedisVersion: String = "5.1.5"

val fabricVersion: String = "1.15.3"

val scribeVersion: String = "3.15.0"

val luceneVersion: String = "9.11.1"

val hikariCPVersion: String = "5.1.0"

val commonsDBCP2Version: String = "2.12.0"

val sqliteVersion: String = "3.46.1.0"

val duckdbVersion: String = "1.0.0"

val h2Version: String = "2.3.232"

val postgresqlVersion: String = "42.7.3"

val catsVersion: String = "3.5.4"

val fs2Version: String = "3.11.0"

val scalaTestVersion: String = "3.2.19"

val catsEffectTestingVersion: String = "1.5.0"

lazy val root = project.in(file("."))
	.aggregate(core.jvm, sql, sqlite, postgresql, duckdb, h2, lucene, halodb, rocksdb, mapdb, redis, async, all)
	.settings(
		name := projectName,
		publish := {},
		publishLocal := {}
	)

lazy val core = crossProject(JVMPlatform) // TODO: Add JSPlatform and NativePlatform
	.crossType(CrossType.Pure)
	.settings(
		name := s"$projectName-core",
		libraryDependencies ++= Seq(
			"com.outr" %%% "scribe" % scribeVersion,
			"org.typelevel" %%% "fabric-io" % fabricVersion,
			"com.outr" %% "scribe-slf4j" % scribeVersion,
			"org.scalatest" %%% "scalatest" % scalaTestVersion % Test
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

lazy val sql = project.in(file("sql"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-sql",
		fork := true,
		libraryDependencies ++= Seq(
			"com.zaxxer" % "HikariCP" % hikariCPVersion,
			"org.apache.commons" % "commons-dbcp2" % commonsDBCP2Version,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val sqlite = project.in(file("sqlite"))
	.dependsOn(sql, core.jvm % "test->test")
	.settings(
		name := s"$projectName-sqlite",
		fork := true,
		libraryDependencies ++= Seq(
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val h2 = project.in(file("h2"))
	.dependsOn(sql, core.jvm % "test->test")
	.settings(
		name := s"$projectName-h2",
		fork := true,
		libraryDependencies ++= Seq(
			"com.h2database" % "h2" % h2Version,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val postgresql = project.in(file("postgresql"))
	.dependsOn(sql, core.jvm % "test->test")
	.settings(
		name := s"$projectName-postgresql",
		fork := true,
		libraryDependencies ++= Seq(
			"org.postgresql" % "postgresql" % postgresqlVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val duckdb = project.in(file("duckdb"))
	.dependsOn(sql, core.jvm % "test->test")
	.settings(
		name := s"$projectName-duckdb",
		fork := true,
		libraryDependencies ++= Seq(
			"org.duckdb" % "duckdb_jdbc" % duckdbVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val lucene = project.in(file("lucene"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-lucene",
		fork := true,
		libraryDependencies ++= Seq(
			"org.apache.lucene" % "lucene-core" % luceneVersion,
			"org.apache.lucene" % "lucene-memory" % luceneVersion,
			"org.apache.lucene" % "lucene-queryparser" % luceneVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val halodb = project.in(file("halodb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-halo",
		fork := true,
		libraryDependencies ++= Seq(
			"com.github.yahoo" % "HaloDB" % haloDBVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val rocksdb = project.in(file("rocksdb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-rocks",
		fork := true,
		libraryDependencies ++= Seq(
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val mapdb = project.in(file("mapdb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-mapdb",
		libraryDependencies ++= Seq(
			"org.mapdb" % "mapdb" % mapdbVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		),
		fork := true
	)

lazy val redis = project.in(file("redis"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-redis",
		libraryDependencies ++= Seq(
			"redis.clients" % "jedis" % jedisVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		),
		fork := true
	)

lazy val async = project.in(file("async"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-async",
		fork := true,
		libraryDependencies ++= Seq(
			"org.typelevel" %% "cats-effect" % catsVersion,
			"co.fs2" %% "fs2-core" % fs2Version
		)
	)

lazy val all = project.in(file("all"))
	.dependsOn(core.jvm, core.jvm % "test->test", sqlite, postgresql, duckdb, h2, lucene, halodb, rocksdb, mapdb, redis, async)
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
	.enablePlugins(JmhPlugin)
	.settings(
		name := s"$projectName-benchmark",
		fork := true,
		libraryDependencies ++= Seq(
			"org.mongodb" % "mongodb-driver-sync" % "5.0.1",
			"org.postgresql" % "postgresql" % "42.7.4",
			"org.mariadb.jdbc" % "mariadb-java-client" % "3.3.3",
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"com.h2database" % "h2" % h2Version,
			"org.apache.derby" % "derby" % "10.17.1.0",
			"commons-io" % "commons-io" % "2.16.1",
			"co.fs2" %% "fs2-io" % "3.9.4",
			"com.outr" %% "scarango-driver" % "3.20.0",
//			"com.outr" %% "lightdb-all" % "0.11.0",
			"org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
			"org.jooq" % "jooq" % "3.19.10",
			"io.quickchart" % "QuickChart" % "1.2.0"
		)
	)