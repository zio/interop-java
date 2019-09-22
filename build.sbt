import BuildHelper._

inThisBuild(
  List(
    organization := "dev.zio",
    homepage := Some(url("https://zio.dev")),
    licenses := List("Apache-2.0" -> url("http://www.apache.org/licenses/LICENSE-2.0")),
    developers := List(
      Developer(
        "jdegoes",
        "John De Goes",
        "john@degoes.net",
        url("http://degoes.net")
      )
    ),
    pgpPassphrase := sys.env.get("PGP_PASSWORD").map(_.toArray),
    pgpPublicRing := file("/tmp/public.asc"),
    pgpSecretRing := file("/tmp/secret.asc"),
    scmInfo := Some(
      ScmInfo(url("https://github.com/zio/interop-java/"), "scm:git:git@github.com:zio/interop-java.git")
    )
  )
)

ThisBuild / publishTo := sonatypePublishToBundle.value

addCommandAlias("fmt", "all scalafmtSbt scalafmt test:scalafmt")
addCommandAlias("check", "all scalafmtSbtCheck scalafmtCheck test:scalafmtCheck")

lazy val java = project
  .in(file("."))
  .enablePlugins(BuildInfoPlugin)
  .settings(stdSettings("zio-interop-java"))
  .settings(buildInfoSettings)
  .settings(testFrameworks += new TestFramework("zio.test.sbt.ZTestFramework"))
  .settings(
    libraryDependencies ++= Seq(
      "dev.zio" %% "zio"          % "1.0.0-RC12-1",
      "dev.zio" %% "zio-test"     % "1.0.0-RC12-1" % Test,
      "dev.zio" %% "zio-test-sbt" % "1.0.0-RC12-1" % Test
    )
  )
