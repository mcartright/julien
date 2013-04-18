package julien
package config

case class BuildParams(
  var indexPath: String,
  var distrib: Option[Int],
  var jobDir: Option[String],
  var port: Option[Int],
  var inputPath: List[String] = List[String](),
  var corpus: CorpusParams = CorpusParams(),
  var fields: FieldParams = FieldParams(),
  var postings: PostingsParams = PostingsParams(),
  var tokenizer: TokenizerParams = TokenizerParams(),
  var mode: String = "local",
  var keepJobDir: Boolean = false,
  var links: Boolean = false
) extends StandardParameters
