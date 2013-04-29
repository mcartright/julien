package julien.cli.examples

import julien.access.Index

/**
 * User: jdalton
 * Date: 4/26/13
 */
object VerifyIndex {


  // verify the correctness of galago vs julien on robust04

  val diskIndex = Index.disk("/usr/dan/users4/jdalton/code/julien/data/test-index")

  diskIndex.collectionLength

}
