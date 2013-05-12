// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStream;

import julien.galago.core.index.BTreeWriter;
import julien.galago.core.index.CompressedByteBuffer;
import julien.galago.core.index.CompressedRawByteBuffer;
import julien.galago.core.index.IndexElement;
import julien.galago.core.types.KeyValuePair;
import julien.galago.core.types.NumberedField;
import julien.galago.tupleflow.IncompatibleProcessorException;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.OutputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Source;
import julien.galago.tupleflow.Step;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.ErrorStore;
import julien.galago.tupleflow.execution.Verification;


/**
 *
 * @author trevor
 */
@InputClass(className = "julien.galago.core.types.NumberedField", order = {"+fieldName", "+number"})
@OutputClass(className = "julien.galago.core.types.KeyValuePair", order = {"+key"})
public class FieldIndexWriter implements NumberedField.FieldNameNumberOrder.ShreddedProcessor,
        Source<KeyValuePair> // parallel index data output
{

  public class ContentList implements IndexElement {

    CompressedByteBuffer header;
    CompressedRawByteBuffer data;
    long lastDocument;
    int documentCount;
    byte[] key;
    byte[] content = null;

    public ContentList(byte[] k) {
      key = k;
      documentCount = 0;
      lastDocument = 0;
      header = new CompressedByteBuffer();
      data = new CompressedRawByteBuffer();
    }

    @Override
    public byte[] key() {
      return key;
    }

    @Override
    public long dataLength() {
      long listLength = 0;

      listLength += data.length();
      listLength += header.length();

      return listLength;
    }

    @Override
    public void write(OutputStream stream) throws IOException {
      header.write(stream);
      header.clear();

      data.write(stream);
      data.clear();
    }

    public void setContent(byte[] s) {
      content = s;
    }

    public void addDocument(long document) {
      if (content != null) {
        data.add(content);
      }
      data.add(document - lastDocument);
      lastDocument = document;
      content = null;
      documentCount++;
    }

    public void close() throws IOException {
      if (content != null) {
        data.add(content);
      }
      header.add(documentCount);
    }
  }
  long minimumSkipListLength = 2048;
  int skipByteLength = 128;
  byte[] lastWord;
  long lastPosition = 0;
  long lastDocument = 0;
  BTreeWriter writer;
  ContentList invertedList;
  OutputStream output;
  long filePosition;
  long documentCount = 0;
  long collectionLength = 0;
  Parameters header;
  TupleFlowParameters stepParameters;
  boolean parallel;
  String filename;

  public FieldIndexWriter(TupleFlowParameters parameters) throws FileNotFoundException, IOException {
    header = parameters.getJSON();
    stepParameters = parameters;
    header.set("readerClass", FieldIndexReader.class.getName());
    header.set("writerClass", getClass().toString());
    filename = header.getString("filename");
  }

  @Override
  public void processFieldName(byte[] wordBytes) throws IOException {
    if (writer == null) {
      writer = new DiskBTreeWriter(stepParameters);
    }

    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);
      invertedList = null;
    }

    invertedList = new ContentList(wordBytes);

    assert lastWord == null || 0 != Utility.compare(lastWord, wordBytes) : "Duplicate word";
    lastWord = wordBytes;
  }

  @Override
  public void processNumber(long document) throws IOException {
    invertedList.addDocument(document);
  }

  @Override
  public void processTuple(byte[] content) throws IOException {
    invertedList.setContent(content);
  }

  @Override
  public void close() throws IOException {
    if (invertedList != null) {
      invertedList.close();
      writer.add(invertedList);
    }
    if (writer != null) {
      writer.close();
    }
  }

  public static void verify(TupleFlowParameters parameters, ErrorStore handler) {
    if (!parameters.getJSON().isString("filename")) {
      handler.addError("ExtentIndexWriter requires a 'filename' parameter.");
      return;
    }

    String index = parameters.getJSON().getString("filename");
    Verification.requireWriteableFile(index, handler);
  }

  @Override
  public Step setProcessor(Step processor) throws IncompatibleProcessorException {
    if (writer instanceof Source) {
      ((Source) writer).setProcessor(processor);
    }
    return processor;
  }
}
