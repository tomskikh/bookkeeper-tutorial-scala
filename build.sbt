name := "bookeeper"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.curator" % "curator-recipes" % "2.12.0",
  "org.apache.curator" % "curator-test" % "2.12.0",
  "org.apache.bookkeeper" % "bookkeeper-server" % "4.4.0",
  "io.zipkin.brave" % "brave" % "4.11.0",
  "io.zipkin.reporter2" % "zipkin-sender-okhttp3" % "2.1.4"
)
