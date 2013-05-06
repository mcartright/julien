//package julien.galago.core.index.disk
//
//import julien.galago.core.index.{KeyListReader, BTreeReader, BTreeIterator}
//import julien.galago.tupleflow.DataStream
//import julien.galago.tupleflow.VByteInput
//
//
///**
// * User: jdalton
// * Date: 5/3/13
// */
//class TermCountReader extends KeyListReader#ListIterator {
//
//  var iterator: BTreeReader#BTreeIterator
//  var documentCount: Int
//  var collectionCount: Int
//  var maximumPositionCount: Int
//  var documents: VByteInput
//  var counts: VByteInput
//  var documentIndex: Int
//  var currentDocument: Int
//  var currentCount: Int
//
//  // Support for resets
//  var startPosition: Long
//  var endPosition: Long
//  // to support skipping
//  var skips: VByteInput
//  var skipPositions: VByteInput
//  var skipPositionsStream: DataStream
//  var documentsStream: DataStream;
//  var countsStream: DataStream;
//  var skipDistance: Int
//  var skipResetDistance: Int
//  var numSkips: Long
//  var skipsRead: Long
//  var nextSkipDocument: Long
//  var lastSkipPosition: Long
//  var documentsByteFloor: Long
//  var countsByteFloor: Long
//
//  def init(i: BTreeIterator) = {
//    iterator = i
//    startPosition = iterator.getValueStart()
//    endPosition = iterator.getValueEnd()
//    key = iterator.getKey()
//
//
//    val valueStream = iterator.getSubValueStream(0, iterator.getValueLength());
//    val stream = new VByteInput(valueStream);
//
//    // metadata
//    val options = stream.readInt()
//    documentCount = stream.readInt()
//    collectionCount = stream.readInt()
//
//    if ((options & HAS_MAXTF) == HAS_MAXTF) {
//      maximumPositionCount = stream.readInt();
//    } else {
//      maximumPositionCount = Integer.MAX_VALUE;
//    }
//
//    if ((options & HAS_SKIPS) == HAS_SKIPS) {
//      skipDistance = stream.readInt();
//      skipResetDistance = stream.readInt();
//      numSkips = stream.readLong();
//    }
//
//    // segment lengths
//    val documentByteLength = stream.readLong();
//    val countsByteLength = stream.readLong();
//    val skipsByteLength = 0;
//    val skipPositionsByteLength = 0;
//
//    if ((options & HAS_SKIPS) == HAS_SKIPS) {
//      skipsByteLength = stream.readLong();
//      skipPositionsByteLength = stream.readLong();
//    }
//
//    val documentStart = valueStream.getPosition();
//    val countsStart = documentStart + documentByteLength;
//    val countsEnd = countsStart + countsByteLength;
//
//    documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
//    countsStream = iterator.getSubValueStream(countsStart, countsByteLength);
//
//    documents = new VByteInput(documentsStream);
//    counts = new VByteInput(countsStream);
//
//    if ((options & HAS_SKIPS) == HAS_SKIPS) {
//
//      val skipsStart = countsEnd;
//      val skipPositionsStart = skipsStart + skipsByteLength;
//      val skipPositionsEnd = skipPositionsStart + skipPositionsByteLength;
//
//      // assert skipPositionsEnd == endPosition - startPosition;
//
//      skips = new VByteInput(iterator.getSubValueStream(skipsStart, skipsByteLength));
//      skipPositionsStream = iterator.getSubValueStream(skipPositionsStart, skipPositionsByteLength);
//      skipPositions = new VByteInput(skipPositionsStream);
//
//      // load up
//      nextSkipDocument = skips.readInt();
//      documentsByteFloor = 0;
//      countsByteFloor = 0;
//    } else {
//      //  assert countsEnd == endPosition - startPosition;
//      skips = null;
//      skipPositions = null;
//    }
//
//    documentIndex = 0;
//    currentDocument += documents.readInt();
//    currentCount = counts.readInt();
//  }
//}
