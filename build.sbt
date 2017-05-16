name := "bookeeper"

version := "1.0"

scalaVersion := "2.12.2"

libraryDependencies ++= Seq(
  "org.apache.curator" % "curator-recipes" % "2.12.0",
  "org.apache.curator" % "curator-test" % "2.12.0",
  "org.apache.bookkeeper" % "bookkeeper-server" % "4.4.0"
)
        