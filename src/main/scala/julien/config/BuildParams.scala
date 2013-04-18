package julien
package config

case class BuildParams(
  var corpus: CorpusParams = CorpusParams(),
  var fields: FieldParams = FieldParams(),
  var inputPath: List[String] = List[String](),
  var postings: PostingsParams = PostingsParams(),
  var tokenizer: TokenizerParams = TokenizerParams(),
  var mode: String = "local",
  var keepJobDir: Boolean = false,
  var links: Boolean = false,
  var distrib: Int,
  var jobDir: String,
  var port: Int,
  var indexPath: String
) extends StandardParameters
