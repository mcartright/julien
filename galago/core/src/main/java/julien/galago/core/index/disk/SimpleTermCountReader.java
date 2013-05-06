package julien.galago.core.index.disk;

import java.io.DataInput;
import java.io.IOException;

import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.KeyListReader;
import julien.galago.tupleflow.DataStream;
import julien.galago.tupleflow.VByteInput;

public class SimpleTermCountReader {

    BTreeReader.BTreeIterator iterator;
    int documentCount;
    int collectionCount;
    int maximumPositionCount;
    VByteInput documents;
    VByteInput counts;

    public int documentIndex;
    public int currentDocument;
    public int currentCount;

    private DataStream countsStream;
    private DataStream documentsStream;

    public SimpleTermCountReader(BTreeReader.BTreeIterator itr)
            throws IOException {
        this.iterator = itr;
        reset();
    }

    protected void reset() throws IOException {
        currentDocument = 0;
        currentCount = 0;
        documentIndex = 0;

        DataStream valueStream = iterator.getSubValueStream(0,
                iterator.getValueLength());
        DataInput stream = new VByteInput(valueStream);

        // metadata
        int options = stream.readInt();
        documentCount = stream.readInt();
        collectionCount = stream.readInt();

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

        if ((options & KeyListReader.ListIterator.HAS_SKIPS) == KeyListReader.ListIterator.HAS_SKIPS) {
            stream.readLong();
            stream.readLong();
        }

        long documentStart = valueStream.getPosition();
        long countsStart = documentStart + documentByteLength;
        long countsEnd = countsStart + countsByteLength;

        documentsStream = iterator.getSubValueStream(documentStart,
                documentByteLength);
        countsStream = iterator
                .getSubValueStream(countsStart, countsByteLength);

        documents = new VByteInput(documentsStream);
        counts = new VByteInput(countsStream);

        documentIndex = 0;
    }

    public void nextDoc() throws IOException {
        documentIndex += 1;
        currentDocument += documents.readInt();
        currentCount = counts.readInt();
    }
}
