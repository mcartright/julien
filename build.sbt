organization := "edu.umass.ciir"

name := "julien"

version := "0.1"

scalaVersion := "2.10.1"

resolvers ++= Seq("Galago" at "http://ayr.cs.umass.edu/m2/repository",
	  "Mvn Repo" at "http://repo1.maven.org/maven2"
)

libraryDependencies ++= Seq(
  "org.apache.commons" % "commons-compress" % "1.0",
  "edu.umass.ciir" % "macros" % "0.1",
  "org.antlr" % "antlr4-runtime" % "4.0",
  "julien.galago" % "core" % "3.3",
  "julien.galago" % "tupleflow" % "3.3",
  "julien.galago" % "tupleflow-typebuilder" % "3.3"
)

excludeFilter in Compile := new SimpleFilter(s =>
  s.contains("learning") || s.contains("garage") || s == "test.scala"
)

scalacOptions in (Compile, doc) ++=
Opts.doc.sourceUrl("https://github.com/CIIR/julien/tree/master/src/main/scala/€{FILE_PATH}.scala") ++
Opts.doc.title("Julien") ++
Seq("-d", "/var/www/julien-docs/julien")

doc in Compile <<= (doc in Compile) map { in =>
  Seq("bash","-c",""" for x in $(find target/scala-2.10/api/ -type f); do sed -i "s_/usr/ayr/tmp1/irmarc/projects/thesis/code/julien/src/main/scala/__" $x; done """).!
  in
}
