name := "spark-sandbox"

version := "0.0.1"

scalaVersion := "2.11.11"

sparkVersion := "2.2.1"
sparkComponents ++= Seq("sql")

libraryDependencies += "com.univocity" % "univocity-parsers" % "2.2.1"
libraryDependencies += "org.apache.hadoop" % "hadoop-common" % "2.7.1" % "provided"
libraryDependencies += "org.scalatest" %% "scalatest" % "3.0.1" % "test"

artifactName := { (sv: ScalaVersion, module: ModuleID, artifact: Artifact) =>
  artifact.name + "_" + sv.binary + "-" + sparkVersion.value + "_" + module.revision + "." + artifact.extension
}
