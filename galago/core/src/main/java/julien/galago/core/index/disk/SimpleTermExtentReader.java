package julien.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.KeyListReader;
import julien.galago.core.util.ExtentArray;
import julien.galago.tupleflow.DataStream;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.VByteInput;

public class SimpleTermExtentReader {

    BTreeReader.BTreeIterator iterator;
    int documentCount;
    int totalPositionCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;
    VByteInput positions;
    int documentIndex;
    
    public int currentDocument;
    public int currentCount;
    public ExtentArray currentPositions;

    private DataStream countsStream;
    private DataStream documentsStream;
    private DataStream positionsStream;
    private int inlineMinimum;
    
    public SimpleTermExtentReader(BTreeReader.BTreeIterator itr) throws IOException{
      this.iterator = itr;
      reset();
    }
    
    protected void reset() throws IOException {
        documentIndex = 0;
        currentDocument = 0;
        currentCount = 0;
        currentPositions = new ExtentArray();

        DataStream valueStream = iterator.getSubValueStream(0, iterator.getValueLength());
        DataInput stream = new VByteInput(valueStream);

        // metadata
        int options = stream.readInt();

        if ((options & KeyListReader.ListIterator.HAS_INLINING) == KeyListReader.ListIterator.HAS_INLINING) {
          inlineMinimum = stream.readInt();
        } else {
          inlineMinimum = Integer.MAX_VALUE;
        }

        documentCount = stream.readInt();
        totalPositionCount = stream.readInt();

        if ((options & KeyListReader.ListIterator.HAS_MAXTF) == KeyListReader.ListIterator.HAS_MAXTF) {
          maximumPositionCount = stream.readInt();
        } else {
          maximumPositionCount = Integer.MAX_VALUE;
        }

        if ((options & KeyListReader.ListIterator.HAS_SKIPS) == KeyListReader.ListIterator.HAS_SKIPS) {
          stream.readInt();
          stream.readInt();
          stream.readLong();
        }

        // segment lengths
        long documentByteLength = stream.readLong();
        long countsByteLength = stream.readLong();
        long positionsByteLength = stream.readLong();
        long skipsByteLength = 0;
        long skipPositionsByteLength = 0;

        if ((options & KeyListReader.ListIterator.HAS_SKIPS) == KeyListReader.ListIterator.HAS_SKIPS) {
          stream.readLong();
          stream.readLong();
        }

        long documentStart = valueStream.getPosition();
        long countsStart = documentStart + documentByteLength;
        long positionsStart = countsStart + countsByteLength;
        long positionsEnd = positionsStart + positionsByteLength;

        documentsStream = iterator.getSubValueStream(documentStart, documentByteLength);
        countsStream = iterator.getSubValueStream(countsStart, countsByteLength);
        positionsStream = iterator.getSubValueStream(positionsStart, positionsByteLength);

        documents = new VByteInput(documentsStream);
        counts = new VByteInput(countsStream);
        positions = new VByteInput(positionsStream);

    }

      public void nextDoc() throws IOException{
        currentDocument += documents.readInt();
        currentCount = counts.readInt();

        currentPositions.reset();
        if (currentCount > inlineMinimum) {
           positions.readInt(); // we are being proactive
        }
       // currentPositions.setDocument(currentDocument);
        int position = 0;
        for (int i = 0; i < currentCount; i++) {
          position += positions.readInt();
          currentPositions.add(position);
        }
      }
}

