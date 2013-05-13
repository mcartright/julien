package julien

/** This module represents more "free-form" access to the underlying
  * index structures.
  *
  * In addition to a fair number of convenience functions avaiable to gather
  * simple statistics, constructs such as iterable sequences (i.e. the
  * Seq trait from scala.collection) are available over Documents, Postings, and
  * Key/Value pairs. I plan to add more to that, time permitting.
  */
package object access {
  import language.implicitConversions

  // Makes byte-array calls much less annoying
  implicit def string2bytes(s: String) =
    julien.galago.tupleflow.Utility.fromString(s)
}
