// Your profile name of the sonatype account. The default is the same with the organization value
sonatypeProfileName := "com.microsoft.spark"

// To sync with Maven central, you need to supply the following information:
publishMavenStyle := true

// Open-source license of your choice
licenses := Seq("MIT" -> url("https://raw.githubusercontent.com/microsoft/OSDU-Spark/main/LICENSE"))

// Where is the source code hosted: GitHub or GitLab?
import xerial.sbt.Sonatype._
sonatypeProjectHosting := Some(GitHubHosting("microsoft", "OSDU-Spark", "marcozo@microsoft.com"))

// or if you want to set these fields manually
homepage := Some(url("https://github.com/microsoft/OSDU-Spark"))
scmInfo := Some(
  ScmInfo(
    url("https://github.com/microsoft/OSDU-Spark"),
    "scm:git@github.com:microsoft/OSDU-Spark.git"
  )
)
developers := List(
  Developer(id="eisber", name="Markus Cozowicz", email="marcozo@microsoft.com", url=url("https://github.com/microsoft/OSDU-Spark")),
  Developer(id="spancholi87", name="Swapnil Pancholi", email="spancholi@microsoft.com", url=url("https://github.com/microsoft/OSDU-Spark"))
)

credentials += Credentials("Sonatype Nexus Repository Manager",
        "oss.sonatype.org",
        sys.env.get("OSSRH_USERNAME").getOrElse(""),
        sys.env.get("OSSRH_TOKEN").getOrElse(""))