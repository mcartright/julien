package julien
package config

case class CorpusParams(
  blockSize: Int = 512,
  mergerClass: String = "julien.galago.core.index.merge.CorpusMerger",
  readerClass: String = "julien.galago.core.index.merge.CorpusReader",
  writerClass: String =
    "julien.galago.core.index.merge.CorpusFolderWriter"
) extends StandardParameters
