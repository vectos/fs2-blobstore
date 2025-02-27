import sbt._
import sbt.Keys._

object Tests extends AutoPlugin {
  override def trigger = allRequirements

  override def buildSettings: Seq[Def.Setting[_]] = Seq(
    libraryDependencies ++= Seq(
      "org.scalatest" %% "scalatest" % "3.2.5" % Test
    )
  )

  override def projectSettings: Seq[Def.Setting[_]] = Seq(
    Test / fork := true
  )
}
