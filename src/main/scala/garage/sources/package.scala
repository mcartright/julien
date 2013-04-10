package garage

/** Immediate predecessor to the current incarnation. The driving idea here
  * is that "Sources" are provided to a set of Feature functions. The
  * Features would be oblivious to the current disposition of the system
  * (e.g. current document being scored, etc), and sources would provide an
  * abstract interface for what information would be provided from the depths
  * of the implementation layer. This idea is still very prevalent in the
  * current Julien incarnation, however the strictness in specifying Views
  * is much more relaxed compared to sources (discussed below).
  *
  * Sources were defined in a very first-order logic fashion, where sources
  * could be totally free, partially bound, or fully bound. Unfortunately
  * the abstraction was overly strict, for example if a data source needed
  * binding of several variables, it was hard to curry them with more
  * partial bindings until the source is fully bound. However the idea of
  * currying partially bound source forward was undermined by the fact that
  * the new source wouldn't know the underlying bindings of the previous
  * source.
  *
  * Knowing what I know about Scala now, this idea *could* work, but I don't
  * think we'd get anything above and beyond the current incarnation other than
  * a cuter way to talk about implementation.
  *
  */
package object sources {
  type DiskIndex = org.lemurproject.galago.core.index.disk.DiskIndex
  type MemoryIndex = org.lemurproject.galago.core.index.mem.MemoryIndex
}
