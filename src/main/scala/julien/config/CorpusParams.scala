package julien
package config

case class CorpusParams(
  blockSize: Int = 512,
  mergerClass: String = "org.lemurproject.galago.core.index.merge.CorpusMerger",
  readerClass: String = "org.lemurproject.galago.core.index.merge.CorpusReader",
  writerClass: String =
    "org.lemurproject.galago.core.index.merge.CorpusFolderWriter"
) extends StandardParameters
