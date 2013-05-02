// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.dynamic;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.Map;

import julien.galago.core.index.AggregateReader;
import julien.galago.core.index.Index;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.NamesReader;
import julien.galago.core.index.NullExtentIterator;
import julien.galago.core.index.AggregateReader.CollectionAggregateIterator;
import julien.galago.core.index.AggregateReader.CollectionStatistics;
import julien.galago.core.index.AggregateReader.IndexPartStatistics;
import julien.galago.core.index.LengthsReader.LengthsIterator;
import julien.galago.core.index.corpus.CorpusReader;
import julien.galago.core.parse.Document;
import julien.galago.core.parse.stem.Porter2Stemmer;
import julien.galago.tupleflow.InputClass;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.TupleFlowParameters;
import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.execution.Verified;


/*
 * Memory Index
 *
 * author: sjh, schiu
 *
 */
@Verified
@InputClass(className = "julien.galago.core.parse.Document")
public class DynamicIndex implements Index, Processor<Document> {

  public boolean stemming, nonstemming, makecorpus, dirty;
  protected int documentNumberOffset, documentCount;
  protected Parameters manifest;
  protected HashMap<String, MemoryIndexPart> parts;
  // haven't got any of these at the moment
  HashMap<String, String> defaultIndexOperators = new HashMap<String, String>();
  HashSet<String> knownIndexOperators = new HashSet<String>();

  public DynamicIndex(Parameters p) throws Exception {
    manifest = p;
    initialize();
  }

  public DynamicIndex() throws Exception {
    manifest = new Parameters();
    manifest.set("stemming", false);
    manifest.set("makecorpus", true);
    initialize();
  }

  public DynamicIndex(TupleFlowParameters parameters) throws Exception {
    manifest = parameters.getJSON();
    initialize();
  }

  private void initialize() throws Exception {
    // determine which parts are to be created:
    stemming = manifest.get("stemming", false);
    nonstemming = manifest.get("nonstemming", true);
    makecorpus = manifest.get("makecorpus", true);

    // we should have either a stemmed or non-stemmed posting list
    assert stemming || nonstemming;

    // this allows memory index to start numbering documents
    // from a specific documentCount.
    documentNumberOffset = (int) manifest.get("documentNumberOffset", 0L);
    documentCount = documentNumberOffset;

    // Load all parts
    parts = new HashMap<String, MemoryIndexPart>();
    Parameters partParams = new Parameters();
    partParams.set("documentNumberOffset", documentNumberOffset);
    parts.put("names", new MemoryDocumentNames(partParams.clone()));
    parts.put("lengths", new MemoryDocumentLengths(partParams.clone()));

    if (makecorpus) {
      parts.put("corpus", new MemoryCorpus(partParams.clone()));
    }
    if (nonstemming) {
      parts.put("all.postings", new MemoryPositionalIndex(partParams.clone()));
    }
    if (stemming) {
      Parameters stemParams = partParams.clone();
      // should change this to support several stemmers...
      stemParams.set("stemmer",
		     manifest.get("stemmer", Porter2Stemmer.class.getName()));
      parts.put("all.postings.porter", new MemoryPositionalIndex(stemParams));
    }
    dirty = false;
  }

  /**
   * Special function to return the number of documents stored in memory
   * @return documentCount
   */
  public int documentsInIndex() {
    return (documentCount - documentNumberOffset);
  }

  @Override
  public void process(Document doc) throws IOException {
    doc.identifier = documentCount;
    for (MemoryIndexPart part : parts.values()) {
      part.addDocument(doc);
    }
    documentCount++;
  }

  /* this Isn't required at the moment
   *public boolean hasChanged() {
   *  return dirty;
   *}
   
  public String getDefaultPart() {
    if (manifest.isString("defaultPart")) {
      String part = manifest.getString("defaultPart");
      if (parts.containsKey(part)) {
        return part;
      }
    }

    // otherwise, try to default
    if (parts.containsKey("all.postings.porter")) {
      return "all.postings.porter";
    }
    if (parts.containsKey("postings")) {
      return "all.postings";
    }
    // otherwise - anything will do.
    return parts.keySet().iterator().next();
  }
  */

  /**
   * Tests to see if a named index part exists.
   *
   * @param partName The name of the index part to check.
   * @return true, if this index has a part called partName, or false otherwise.
   */
  @Override
  public boolean containsPart(String partName) {
    return parts.containsKey(partName);
  }

  @Override
  public MemoryIndexPart getIndexPart(String partName) {
    return parts.get(partName);
  }

  @Override
  public Iterator getIterator(byte[] key, Parameters p) throws IOException {
    Iterator result = null;
    IndexPartReader part = getIndexPart(p.getString("part"));
    if (part != null) {
      result = part.getIterator(key);
      // modify(result, node);
      if (result == null) {
        result = new NullExtentIterator(key);
      }
    }
    return result;
  }

  /**
   * WARNING: this function returns a static picture of the collection stats.
   * You should NEVER cache this object.
   *
   * @param part
   * @return
   */
  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    if (parts.containsKey(part)) {
      IndexPartReader p = parts.get(part);
      if (AggregateReader.AggregateIndexPart.class.isInstance(p)) {
        return ((AggregateReader.AggregateIndexPart) p).getStatistics();
      }
      throw new IllegalArgumentException("Index part, " + part + ", does not store aggregated statistics.");
    }
    throw new IllegalArgumentException("Index part, " + part + ", could not be found in memory index.");
  }

  @Override
  public void close() throws IOException {
      // Does nothing here
  }

  @Override
  public boolean containsIdentifier(int document) throws IOException {
    NamesReader.NamesIterator ni = this.getNamesIterator();
    ni.syncTo(document);
    return ni.getCurrentIdentifier() == document;
  }

  @Override
  public int getLength(int document) throws IOException {
    return ((MemoryDocumentLengths) parts.get("lengths")).getLength(document);
  }

  @Override
  public String getName(int document) throws IOException {
    return ((MemoryDocumentNames) parts.get("names")).getDocumentName(document);
  }

  @Override
  public int getIdentifier(String document) throws IOException {
    return ((MemoryDocumentNames) parts.get("names")).getDocumentIdentifier(document);
  }

  @Override
  public Document getItem(String document, Parameters p) throws IOException {
    if (parts.containsKey("corpus")) {
      try {
        CorpusReader corpus = (CorpusReader) parts.get("corpus");
        int docId = getIdentifier(document);
        corpus.getDocument(docId, p);
      } catch (Exception e) {
        // ignore the exception
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getItems(List<String> documents, Parameters p) throws IOException {
    HashMap<String, Document> results = new HashMap();

    // should get a names iterator + sort requested documents
    for (String name : documents) {
      results.put(name, getItem(name, p));
    }
    return results;
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    assert(parts != null);
    assert(parts.containsKey("lengths"));
    assert(parts.get("lengths") != null);
    return ((MemoryDocumentLengths) parts.get("lengths")).getLengthsIterator();
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return ((MemoryDocumentNames) parts.get("names")).getNamesIterator();
  }

  @Override
  public Parameters getManifest() {
    return manifest;
  }

  @Override
  public Set<String> getPartNames() {
    return parts.keySet();
  }

  @Override
  public CollectionStatistics getCollectionStatistics(String part) {
    try {
      return ((CollectionAggregateIterator)parts.get("lengths").getIterator(Utility.fromString(part))).getStatistics();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }
}