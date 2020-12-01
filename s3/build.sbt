name := "s3"

val fs2Version = "3.0.0-M8"

libraryDependencies ++= Seq(
  "software.amazon.awssdk" % "s3"                        % "2.15.82",
  "co.fs2"                %% "fs2-reactive-streams"      % fs2Version,
  "com.dimafeng"          %% "testcontainers-scala-core" % "0.39.1" % Test
)
