// Scala versions
val scala213 = "2.13.5"
val scala212 = "2.12.13"
val scala3 = "3.0.0-RC2"
val scala2 = List(scala213, scala212)
val allScalaVersions = scala2 //scala3 :: scala2 // TODO: Switch back after everything is upgraded to RC3
val scalaJVMVersions = allScalaVersions
val scalaJSVersions = allScalaVersions
val scalaNativeVersions = scala2

// Variables
val org: String = "com.outr"
val projectName: String = "lightdb"
val githubOrg: String = "outr"
val email: String = "matt@matthicks.com"
val developerId: String = "darkfrog"
val developerName: String = "Matt Hicks"
val developerURL: String = "http://matthicks.com"

name := projectName
ThisBuild / organization := org
ThisBuild / version := "0.2.0-SNAPSHOT"
ThisBuild / scalaVersion := scala213
ThisBuild / scalacOptions ++= Seq("-unchecked", "-deprecation")
ThisBuild / javacOptions ++= Seq("-source", "1.8", "-target", "1.8")

ThisBuild / resolvers += "yahoo-bintray" at "https://yahoo.bintray.com/maven"

ThisBuild / publishTo := sonatypePublishTo.value
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

val collectionCompatVersion: String = "2.4.3"
val haloDBVersion: String = "0.5.3"
val catsEffectVersion: String = "3.1.0"
val fabricVersion: String = "1.0.5"
val lucene4sVersion: String = "1.11.0"
val fs2Version: String = "3.0.3"
val scribeVersion: String = "3.5.4"

val testyVersion: String = "1.0.5"

lazy val root = project.in(file("."))
	.aggregate(core.js, core.jvm, lucene, halo, all)
	.settings(
		name := projectName,
		publish := {},
		publishLocal := {}
	)

lazy val core = crossProject(JSPlatform, JVMPlatform)
	.crossType(CrossType.Full)
	.settings(
		name := s"$projectName-core",
		libraryDependencies ++= Seq(
			"com.outr" %%% "scribe" % scribeVersion,
			"org.typelevel" %%% "cats-effect" % catsEffectVersion,
			"com.outr" %%% "fabric-parse" % fabricVersion,
			"co.fs2" %%% "fs2-core" % fs2Version,
			"com.outr" %%% "testy" % testyVersion % Test
		),
		testFrameworks += new TestFramework("munit.Framework"),
		libraryDependencies ++= (
			if (scalaVersion.value.startsWith("3.0")) {
				Nil
			} else {
				Seq(
					"org.scala-lang.modules" %% "scala-collection-compat" % collectionCompatVersion,
					"org.scala-lang" % "scala-reflect" % scalaVersion.value
				)
			}
			),
		Compile / unmanagedSourceDirectories ++= {
			val major = if (scalaVersion.value.startsWith("3.0")) "-3" else "-2"
			List(CrossType.Pure, CrossType.Full).flatMap(
				_.sharedSrcDir(baseDirectory.value, "main").toList.map(f => file(f.getPath + major))
			)
		}
	)
	.jsSettings(
		crossScalaVersions := scalaJSVersions,
		Test / scalaJSLinkerConfig ~= { _.withModuleKind(ModuleKind.CommonJSModule) }
	)
	.jvmSettings(
		crossScalaVersions := scalaJVMVersions
	)

lazy val lucene = project.in(file("lucene"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-lucene",
		libraryDependencies ++= Seq(
			"com.outr" %% "lucene4s" % lucene4sVersion,
			"com.outr" %% "testy" % testyVersion % Test
		),
		crossScalaVersions := scalaJVMVersions,
		testFrameworks += new TestFramework("munit.Framework")
	)

lazy val halo = project.in(file("halo"))
	.dependsOn(core.jvm)
	.settings(
		name := s"$projectName-halo",
		libraryDependencies ++= Seq(
			"com.oath.halodb" % "halodb" % haloDBVersion,
			"com.outr" %% "testy" % testyVersion % Test
		),
		fork := true,
		crossScalaVersions := scalaJVMVersions,
		testFrameworks += new TestFramework("munit.Framework")
	)

lazy val all = project.in(file("all"))
	.dependsOn(lucene, halo)
	.settings(
		name := s"$projectName-all",
		libraryDependencies ++= Seq(
			"com.outr" %% "testy" % testyVersion % Test
		),
		fork := true,
		crossScalaVersions := scalaJVMVersions,
		testFrameworks += new TestFramework("munit.Framework")
	)