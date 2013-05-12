// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;
import java.io.DataInputStream;
import java.io.ByteArrayInputStream;

import julien.galago.core.index.BTreeWriter;
import julien.galago.core.index.CompressedByteBuffer;
import julien.galago.core.index.CompressedRawByteBuffer;
import julien.galago.core.index.IndexElement;
import julien.galago.core.index.KeyListReader;
import julien.galago.core.types.NumberedField;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorStore;
import julien.galago.tupleflow.execution.Verification;

import org.xerial.snappy.SnappyInputStream;

/**
 * Provides a column-oriented view of the fields stored during indexing.
 * Since this is a first pass, only going to support inline skipping,
 * which is, for entries greater than a certain number, we place the
 * length before the block, allowing us to skip the entry without decoding
 * should we want to. Easy to support - higher-level skips can come later.
 *
 * @author irmarc
 */
@InputClass(className = "julien.galago.core.types.NumberedField", order = {"+fieldName", "+number"})
public class PositionContentWriter
    implements NumberedField.FieldNameNumberOrder.ShreddedProcessor {

  static final int MARKER_MINIMUM = 2;
  // writer variables //
  Parameters actualParams;
  BTreeWriter writer;
  PositionsList invertedList;
  // statistics //
  byte[] lastWord;

  // skipping parameters
  int options = 0;

  /**
   * Creates a new instance of the PositionContentWriter.
   */
  public PositionContentWriter(TupleFlowParameters parameters)
      throws FileNotFoundException, IOException {
    actualParams = parameters.getJSON();
    actualParams.set("writerClass", getClass().getName());
    actualParams.set("readerClass", PositionContentReader.class.getName());
    writer = new DiskBTreeWriter(parameters);

    // Record max number of occurrences and do inline skipping.
    options |= KeyListReader.ListIterator.HAS_MAXTF;
    options |= KeyListReader.ListIterator.HAS_INLINING;
  }

  @Override
  public void processFieldName(byte[] wordBytes) throws IOException {
    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);

      invertedList = null;
    }

    invertedList = new PositionsList();
    invertedList.setWord(wordBytes);
    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;
  }

  @Override
  public void processNumber(long document) throws IOException {
      invertedList.addDocument((int) document);
  }

  @Override
  public void processTuple(byte[] content) throws IOException {
    invertedList.addContent(content);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);
    }

    // Add stats to the manifest if needed
    Parameters manifest = writer.getManifest();
    writer.close();
  }

  public static void verify(TupleFlowParameters parameters,
			    ErrorStore handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("PositionContentWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  /**
   * Posting list for the column-oriented storage of fields - the content itself
   * is stored in this structure:
   *
   * fieldName -> 3 lists of postings, interleaved as follows:
   * - documents/counts: a list of document/count pairs. No inline skips.
   *       doc numbers are d-gapped.
   * - positions:
   *       the locations where the content occurs. inline skips @ length 2
   *       d-gapped
   * - content:
   *       the content of the field instances. inline skips @ length 2
   */
  public class PositionsList implements IndexElement {

    private long lastDocument;
    private int documentCount;
    private int maximumPositionCount;
    private int totalPositionCount;
    public byte[] word;
    public CompressedByteBuffer header;
    public CompressedRawByteBuffer documents;
    public CompressedRawByteBuffer positions;
    public CompressedRawByteBuffer entries;

    public PositionsList() {
      documents = new CompressedRawByteBuffer();
      positions = new CompressedRawByteBuffer();
      entries = new CompressedRawByteBuffer();
      header = new CompressedByteBuffer();
    }

    /**
     * Close the posting list by finishing off counts and completing header
     * data.
     *
     * @throws IOException
     */
    public void close() throws IOException {
      header.add(options);

      // Start with the inline length
      header.add(MARKER_MINIMUM);

      header.add(documentCount);
      header.add(totalPositionCount);
      header.add(maximumPositionCount);

      header.add(documents.length());
      header.add(positions.length());
      header.add(entries.length());
    }

    /**
     * The length of the posting list. This is the sum of the docid, count, and
     * length buffers plus the skip buffers (if they exist).
     *
     * @return
     */
    @Override
    public long dataLength() {
      long listLength = 0;

      listLength += header.length();
      listLength += positions.length();
      listLength += entries.length();
      listLength += documents.length();
      return listLength;
    }

    /**
     * Write this PositionsList to the provided OutputStream object.
     *
     * @param output
     * @throws IOException
     */
    @Override
    public void write(final OutputStream output) throws IOException {
      header.write(output);
      header.clear();

      documents.write(output);
      documents.clear();

      positions.write(output);
      positions.clear();

      entries.write(output);
      entries.clear();
    }

    /**
     * Return the key for this PositionsList. This will be the set of bytes used
     * to access this posting list after the index is completed.
     *
     * @return
     */
    @Override
    public byte[] key() {
      return word;
    }

    /**
     * Sets the key for this PositionsList, and resets all internal buffers.
     * Should be named 'setKey'.
     *
     * @param word
     */
    public void setWord(byte[] word) {
      this.word = word;
      this.lastDocument = 0;
      this.totalPositionCount = 0;
      this.maximumPositionCount = 0;
    }

    /**
     * Add a new document id to the PositionsList. Assumes there will be at
     * least one length added afterwards (otherwise why add the docid?).
     *
     * @param documentID
     * @throws IOException
     */
    public void addDocument(long documentID) throws IOException {
      documents.add(documentID - lastDocument);
      lastDocument = documentID;
      documentCount++;
    }

    /**
     * Adds the content buffer (containing positions and entries)
     * to the posting list. This is a 1-to-1 with the document ids,
     * so we don't need to lag adding these to their respective
     * buffers.
     *
     * @param position
     * @throws IOException
     */
    public void addContent(byte[] content) throws IOException {
	ByteArrayInputStream bais = new ByteArrayInputStream(content);
	DataInputStream input =
	    new DataInputStream(new SnappyInputStream(bais));
	int positionCount = input.readInt();
	documents.add(positionCount);

	int positionSizeInBytes = input.readInt();
	byte[] posBytes = new byte[positionSizeInBytes];
	input.read(posBytes);

	int entriesSizeInBytes = input.readInt();
	byte[] contentBytes = new byte[entriesSizeInBytes];
	input.read(contentBytes);

        if (positionCount > MARKER_MINIMUM) {
	  positions.add(positionSizeInBytes);
	  entries.add(entriesSizeInBytes);
        }
        positions.add(posBytes);
	entries.add(contentBytes);

	totalPositionCount += positionCount;
        maximumPositionCount = Math.max(maximumPositionCount, positionCount);
    }
  }
}
