name := """q"""

version := "1.0"

scalaVersion := "2.11.11"

libraryDependencies ++= Seq(
  "com.typesafe.akka" %% "akka-actor" % "2.4.7",
  "com.lihaoyi" %% "utest" % "0.4.7" % "test"
)

testFrameworks += new TestFramework("qtest.Framework")

