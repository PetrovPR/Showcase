import sbtassembly.AssemblyKeys.{assembly, assemblyMergeStrategy, mergeStrategy}
import sbtassembly.AssemblyPlugin.assemblySettings
import sbtassembly.AssemblyPlugin.autoImport.MergeStrategy
import sbtassembly.PathList


name := "SparkTestTask"

version := "0.1"

scalaVersion := "2.11.12"

libraryDependencies += "org.apache.spark" %% "spark-core" % "2.4.5"
libraryDependencies += "org.apache.spark" %% "spark-sql" % "2.4.5"

assemblyMergeStrategy in assembly := {
  case PathList("META-INF", xs @ _*) => MergeStrategy.discard
  case x => MergeStrategy.first
}