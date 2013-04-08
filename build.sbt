organization := "edu.umass.ciir"

name := "julien"

version := "0.1"

scalaVersion := "2.10.1"

resolvers += "Galago" at "http://ayr.cs.umass.edu/m2/repository"

libraryDependencies ++= Seq(
  "edu.umass.ciir" % "macros" % "0.1",
  "org.antlr" % "antlr4-runtime" % "4.0",
  "org.lemurproject.galago" % "core" % "3.3-JULIEN",
  "org.lemurproject.galago" % "tupleflow" % "3.3-JULIEN",
  "org.lemurproject.galago" % "tupleflow-typebuilder" % "3.3-JULIEN"
)

scalacOptions in (Compile, doc) ++= Opts.doc.sourceUrl("https://github.com/CIIR/julien/tree/master/src/main/scala/€{FILE_PATH}.scala")

doc in Compile <<= (doc in Compile) map { in =>
  Seq("bash","-c",""" for x in $(find target/scala-2.10/api/ -type f); do sed -i "s_/usr/ayr/tmp1/irmarc/projects/julien/src/main/scala/__" $x; done """).!
  in
}
