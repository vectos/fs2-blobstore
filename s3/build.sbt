name := "s3"

val fs2Version = "2.3.0"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                    % "2.11.4",
  "co.fs2"                 %% "fs2-reactive-streams" % fs2Version
)
