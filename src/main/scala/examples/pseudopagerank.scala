import org.lemurproject.galago.core.index.disk.DiskIndex
import org.lemurproject.galago.core.retrieval.iterator.MovableLengthsIterator
import org.lemurproject.galago.core.retrieval.processing.ScoringContext
import scala.util.Random

val aquaint = Sources.get('aquaint)
val lengths = aquaint.getLengthsIterator
val ctx = new ScoringContext()
lengths.setContext(ctx)
while (!lengths.isDone) {
  val candidate = lengths.getCurrentIdentifier
  ctx.document = candidate
  if (candidate > 0 && lengths.getCurrentLength > 0) {
    Console.printf("%s\t%f\n", aquaint.getName(candidate), Random.nextGaussian)
  }
  lengths.movePast(candidate)
}
