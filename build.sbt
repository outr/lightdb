// Scala versions
val scala213 = "2.13.17"
val scala3 = "3.7.3"
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
ThisBuild / version := "4.10.0"
ThisBuild / scalaVersion := scala3
ThisBuild / crossScalaVersions := allScalaVersions
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")
ThisBuild / scalacOptions ++= {
	if (scalaVersion.value.startsWith("3")) Seq(
		"-Wconf:any:silent"
	)
	else Nil
}

publishMavenStyle := true

ThisBuild / sonatypeCredentialHost := xerial.sbt.Sonatype.sonatypeCentralHost
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

ThisBuild / outputStrategy := Some(StdoutOutput)

Global / excludeFilter := (Global / excludeFilter).value ||
  HiddenFileFilter || "db" || "upload" || "logs" || "target" || "data" || "benchmark"

ThisBuild / javaOptions ++= Seq(
	"--enable-native-access=ALL-UNNAMED",
	"--add-opens=java.base/java.nio=ALL-UNNAMED",
	"--add-opens=java.base/sun.nio.ch=ALL-UNNAMED",
	"--add-modules", "jdk.incubator.vector",
  "--illegal-access=permit",
	"--add-exports=java.base/jdk.internal.ref=ALL-UNNAMED",
	"--add-exports=java.base/sun.nio.ch=ALL-UNNAMED",
	"--add-exports=jdk.unsupported/sun.misc=ALL-UNNAMED",
	"--add-exports=jdk.compiler/com.sun.tools.javac.file=ALL-UNNAMED",
	"--add-opens=jdk.compiler/com.sun.tools.javac=ALL-UNNAMED",
	"--add-opens=java.base/java.lang=ALL-UNNAMED",
	"--add-opens=java.base/java.lang.reflect=ALL-UNNAMED",
	"--add-opens=java.base/java.io=ALL-UNNAMED",
	"--add-opens=java.base/java.util=ALL-UNNAMED"
)

ThisBuild / Test / testOptions += Tests.Argument(TestFrameworks.ScalaTest, "-oDF")
ThisBuild / Test / parallelExecution := false
ThisBuild / Test / testOptions += Tests.Argument("-n", "spec.EmbeddedTest")

val collectionCompatVersion: String = "2.14.0"

val reactifyVersion: String = "4.1.5"

val rapidVersion: String = "2.0.0"

val spatial4JVersion: String = "0.8"

val jtsVersion: String = "1.20.0"

val haloDBVersion: String = "0.5.7"

val rocksDBVersion: String = "10.4.2"

val mapdbVersion: String = "3.1.0"

val lmdbVersion: String = "0.9.1"

val jedisVersion: String = "7.0.0"

val fabricVersion: String = "1.18.3"

val scribeVersion: String = "3.17.0"

val luceneVersion: String = "10.3.0"

val hikariCPVersion: String = "7.0.2"

val commonsDBCP2Version: String = "2.13.0"

val sqliteVersion: String = "3.50.3.0"

val duckdbVersion: String = "1.4.0.0"

val h2Version: String = "2.3.232"

val postgresqlVersion: String = "42.7.7"

val chronicleMapVersion: String = "3.27ea1"

val scalaTestVersion: String = "3.2.19"

lazy val root = project.in(file("."))
	.aggregate(core.jvm, sql, sqlite, postgresql, duckdb, h2, lucene, halodb, rocksdb, mapdb, lmdb, chronicleMap, redis, all)
	.settings(
		name := projectName,
		publish := {},
		publishLocal := {}
	)

lazy val core = crossProject(JVMPlatform)
	.crossType(CrossType.Pure)
	.settings(
		name := s"$projectName-core",
		libraryDependencies ++= Seq(
			"com.outr" %%% "scribe" % scribeVersion,
			"org.typelevel" %%% "fabric-io" % fabricVersion,
			"com.outr" %% "scribe-slf4j" % scribeVersion,
			"org.locationtech.spatial4j" % "spatial4j" % spatial4JVersion,
			"org.locationtech.jts" % "jts-core" % jtsVersion,
			"com.outr" %%% "rapid-core" % rapidVersion,
			"com.outr" %%% "rapid-scribe" % rapidVersion,
			"org.scalatest" %%% "scalatest" % scalaTestVersion % Test,
			"com.outr" %%% "rapid-test" % rapidVersion % Test
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
		fork := true,
		Test / fork := true
	)

lazy val sql = project.in(file("sql"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-sql",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"com.zaxxer" % "HikariCP" % hikariCPVersion,
			"org.apache.commons" % "commons-dbcp2" % commonsDBCP2Version,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val sqlite = project.in(file("sqlite"))
	.dependsOn(sql, sql % "test->test")
	.settings(
		name := s"$projectName-sqlite",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val h2 = project.in(file("h2"))
	.dependsOn(sql, sql % "test->test")
	.settings(
		name := s"$projectName-h2",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"com.h2database" % "h2" % h2Version,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val postgresql = project.in(file("postgresql"))
	.dependsOn(sql, sql % "test->test")
	.settings(
		name := s"$projectName-postgresql",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.postgresql" % "postgresql" % postgresqlVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val duckdb = project.in(file("duckdb"))
	.dependsOn(sql, sql % "test->test")
	.settings(
		name := s"$projectName-duckdb",
		fork := true,
		Test / fork := true,
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
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.apache.lucene" % "lucene-core" % luceneVersion,
			"org.apache.lucene" % "lucene-memory" % luceneVersion,
			"org.apache.lucene" % "lucene-queryparser" % luceneVersion,
			"org.apache.lucene" % "lucene-facet" % luceneVersion,
			"org.apache.lucene" % "lucene-highlighter" % luceneVersion,
			"org.apache.lucene" % "lucene-backward-codecs" % luceneVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val halodb = project.in(file("halodb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-halo",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"com.outr" % "halodb-revive" % haloDBVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val rocksdb = project.in(file("rocksdb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-rocks",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion,
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion classifier "linux64",
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion classifier "osx",
			"org.rocksdb" % "rocksdbjni" % rocksDBVersion classifier "win64",
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
		fork := true,
		Test / fork := true
	)

lazy val lmdb = project.in(file("lmdb"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-lmdb",
		libraryDependencies ++= Seq(
			"org.lmdbjava" % "lmdbjava" % lmdbVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		),
		fork := true,
		Test / fork := true
	)

lazy val chronicleMap = project.in(file("chronicleMap"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-chroniclemap",
		libraryDependencies ++= Seq(
			"net.openhft" % "chronicle-map" % chronicleMapVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		),
		fork := true,
		Test / fork := true
	)

lazy val redis = project.in(file("redis"))
	.dependsOn(core.jvm, core.jvm % "test->test")
	.settings(
		name := s"$projectName-redis",
		libraryDependencies ++= Seq(
			"redis.clients" % "jedis" % jedisVersion,
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		),
		fork := true,
		Test / fork := true
	)

lazy val all = project.in(file("all"))
	.dependsOn(core.jvm, core.jvm % "test->test", sqlite, postgresql, duckdb, h2, lucene, halodb, rocksdb, mapdb, lmdb, chronicleMap, redis)
	.settings(
		name := s"$projectName-all",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.scalatest" %% "scalatest" % scalaTestVersion % Test
		)
	)

lazy val benchmark = project.in(file("benchmark"))
	.dependsOn(all)
	.enablePlugins(JmhPlugin)
	.settings(
		name := s"$projectName-benchmark",
		fork := true,
		Test / fork := true,
		libraryDependencies ++= Seq(
			"org.mongodb" % "mongodb-driver-sync" % "5.0.1",
			"org.postgresql" % "postgresql" % "42.7.4",
			"org.mariadb.jdbc" % "mariadb-java-client" % "3.3.3",
			"org.xerial" % "sqlite-jdbc" % sqliteVersion,
			"com.h2database" % "h2" % h2Version,
			"org.apache.derby" % "derby" % "10.17.1.0",
			"commons-io" % "commons-io" % "2.16.1",
			"co.fs2" %% "fs2-io" % "3.9.4",
//			"com.outr" %% "lightdb-all" % "0.11.0",
			"org.scala-lang.modules" %% "scala-parallel-collections" % "1.0.4",
			"org.jooq" % "jooq" % "3.19.10",
			"io.quickchart" % "QuickChart" % "1.2.0"
		)
	)

lazy val docs = project
	.in(file("documentation"))
	.dependsOn(all)
	.enablePlugins(MdocPlugin)
	.settings(
	  mdocVariables := Map(
			"VERSION" -> version.value
	  ),
	  mdocOut := file(".")
	)