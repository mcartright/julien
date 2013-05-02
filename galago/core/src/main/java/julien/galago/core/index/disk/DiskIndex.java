// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index.disk;

import java.io.File;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import julien.galago.core.index.BTreeFactory;
import julien.galago.core.index.BTreeReader;
import julien.galago.core.index.Index;
import julien.galago.core.index.Iterator;
import julien.galago.core.index.LengthsReader;
import julien.galago.core.index.NamesReader;
import julien.galago.core.index.NullExtentIterator;
import julien.galago.core.index.AggregateReader.AggregateIndexPart;
import julien.galago.core.index.AggregateReader.CollectionAggregateIterator;
import julien.galago.core.index.AggregateReader.CollectionStatistics;
import julien.galago.core.index.AggregateReader.IndexPartStatistics;
import julien.galago.core.index.LengthsReader;
import julien.galago.core.index.LengthsReader.LengthsIterator;
import julien.galago.core.index.corpus.CorpusReader;
import julien.galago.core.parse.Document;
import julien.galago.tupleflow.Parameters;
import julien.galago.tupleflow.Utility;


/**
 * This is the main class for a disk based index structure
 *
 * A index is a set of parts and modifiers Each part is a index file that offers
 * one or more iterators Each modifier alters or extends the data provided by
 * the corresponding part Queries are be processed in this class using a tree of
 * iterators
 *
 * See the Index interface for the list of public functions.
 *
 * @author trevor, sjh, irmarc
 */
public class DiskIndex implements Index {

  protected static final String manifestName = "buildManifest.json";
  protected File location;
  protected Parameters manifest;
  protected Set<String> partNames;
  protected Map<String, IndexPartReader> loadedParts;

  public DiskIndex(String indexPath) throws IOException {
    // Make sure it's a valid location
    location = new File(indexPath);
    if (!location.isDirectory()) {
      throw new IOException(String.format("%s is not a directory.", indexPath));
    }

    // Just find the parts, and point to them. Open stuff up later.
    partNames = new HashSet<String>(Arrays.asList(location.list()));
    if (!partNames.contains(manifestName)) {
      throw new IOException(String.format("Did not find part file %s. Dying!",
              manifestName));
    }

    // Load the manifest - the others are a bit bigger, so be lazy with them
    String manifestPath = new File(location, manifestName).getAbsolutePath();
    manifest = Parameters.parse(new File(location, manifestName));
    partNames.remove(manifestName);

    // Set up other structures
    loadedParts = new HashMap<String, IndexPartReader>();
  }

  protected BTreeReader getBTree(String path) throws IOException {
    BTreeReader reader = BTreeFactory.getBTreeReader(path);
    return reader;
  }

  protected <T extends Index.IndexPartReader> T getPart(String partname)
          throws IOException {
    if (!loadedParts.containsKey(partname)) {
      if (!partNames.contains(partname)) {
        throw new IOException(String.format("Did not find part file %s. Dying!",
                partname));
      }

      // try to load it
      String fullPath = location.getAbsolutePath() + File.separator + partname;
      BTreeReader reader = getBTree(fullPath);
      if (reader == null) {
        throw new RuntimeException("Unable to load BTree: " + fullPath);
      }
      IndexPartReader ipr = openIndexReader(reader);
      if (ipr == null) {
        throw new RuntimeException("Unable to create part reader:" + fullPath);
      }
      loadedParts.put(partname, ipr);
    }

    try {
      return ((T) loadedParts.get(partname));
    } catch (ClassCastException cce) {
      throw new IllegalArgumentException("Cannot cast" + partname, cce);
    }
  }

  public File getIndexLocation() {
    return location;
  }

  @Override
  public IndexPartReader getIndexPart(String part) throws IOException {
    return this.<Index.IndexPartReader>getPart(part);
  }

  /**
   * Tests to see if a named index part exists.
   *
   * @param partName The name of the index part to check.
   * @return true, if this index has a part called partName, or false otherwise.
   */
  @Override
  public boolean containsPart(String partName) {
    return partNames.contains(partName);
  }

  @Override
  public boolean containsIdentifier(int document) throws IOException {
    NamesReader.NamesIterator ni = getNamesIterator();
    ni.syncTo(document);
    return ni.hasMatch(document);
  }

  @Override
  public Iterator getIterator(byte[] key, Parameters p) throws IOException {
    Iterator result = null;
    IndexPartReader part = getIndexPart(p.getString("part"));
    if (part != null) {
      result = part.getIterator(key);
      if (result == null) {
        result = new NullExtentIterator(key);
      }
    }
    return result;
  }

  @Override
  public CollectionStatistics getCollectionStatistics(String field) {
    try {
      LengthsReader l = this.<LengthsReader>getPart("lengths");
      return ((CollectionAggregateIterator) l.getIterator(Utility.fromString(field))).getStatistics();
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    try {
      IndexPartReader p = this.<IndexPartReader>getPart(part);
      if (AggregateIndexPart.class.isInstance(p)) {
        return ((AggregateIndexPart) p).getStatistics();
      }
      throw new IllegalArgumentException("Index part, " + part + ", does not store aggregated statistics.");
    } catch (IOException ioe) {
      throw new RuntimeException(ioe);
    }
  }

  @Override
  public void close() throws IOException {
    for (IndexPartReader part : loadedParts.values()) {
      part.close();
    }
    loadedParts.clear();
  }

  @Override
  public int getLength(int document) throws IOException {
    return this.<LengthsReader>getPart("lengths").getLength(document);
  }

  @Override
  public String getName(int document) throws IOException {
    return this.<NamesReader>getPart("names").getDocumentName(document);
  }

  @Override
  public int getIdentifier(String document) throws IOException {
    return this.<NamesReader>getPart("names.reverse").getDocumentIdentifier(document);
  }

  @Override
  public Document getItem(String name, Parameters p) throws IOException {
    if (partNames.contains("corpus")) {
      try {
        CorpusReader corpus = this.<CorpusReader>getPart("corpus");
        int docId = getIdentifier(name);
        return corpus.getDocument(docId, p);
      } catch (Exception e) {
        // ignore the exception
        Logger.getLogger(this.getClass().getName()).log(Level.SEVERE,
                "Failed to get document: {0}\n{1}",
                new Object[]{name, e.toString()});
      }
    }
    return null;
  }

  @Override
  public Map<String, Document> getItems(List<String> names, Parameters p) throws IOException {
    HashMap<String, Document> results = new HashMap();

    // should get a names iterator + sort requested documents
    for (String name : names) {
      results.put(name, getItem(name, p));
    }
    return results;
  }

  @Override
  public LengthsIterator getLengthsIterator() throws IOException {
    return this.<LengthsReader>getPart("lengths").getLengthsIterator();
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return this.<NamesReader>getPart("names").getNamesIterator();
  }

  @Override
  public Parameters getManifest() {
    return manifest;
  }

  @Override
  public Set<String> getPartNames() {
    return partNames;
  }

  /**
   * Workhorse method for actually loading an IndexPartReader.
   *
   * @param path The full path to the part to open.
   * @return If successful, the opened part.
   * @throws IOException
   */
  public static IndexPartReader openIndexReader(String path)
          throws IOException {
    BTreeReader reader = BTreeFactory.getBTreeReader(path);
    return openIndexReader(reader);
  }

  public static IndexPartReader openIndexReader(BTreeReader reader)
          throws IOException {
    String path = reader.getManifest().get("filename", "unknown");
    // if it's not an index: return null
    if (reader == null) {
      return null;
    }

    if (!reader.getManifest().isString("readerClass")) {
      throw new IOException("Tried to open an index part at " + path + ", but the "
              + "file has no readerClass specified in its manifest. "
              + "(the readerClass is the class that knows how to decode the "
              + "contents of the file)");
    }

    String className = reader.getManifest().getString("readerClass");
    Class readerClass;
    try {
      readerClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + ", which was specified as the readerClass "
              + "in " + path + ", could not be found.");
    }

    if (!IndexPartReader.class.isAssignableFrom(readerClass)) {
      throw new IOException("Somehow " + className
              + " is not an Index.IndexPartReader subclass.");
    }

    Constructor c;
    try {
      c = readerClass.getConstructor(BTreeReader.class);
    } catch (NoSuchMethodException ex) {
      throw new IOException(className + " has no constructor that takes a single "
              + "IndexReader argument.");
    } catch (SecurityException ex) {
      throw new IOException(className + " doesn't have a suitable constructor that "
              + "this code has access to (SecurityException)");
    }

    IndexPartReader componentReader;
    try {
      componentReader = (IndexPartReader) c.newInstance(reader);
    } catch (Exception ex) {
      throw new IOException("Caught a generic exception while instantiating "
              + "a StructuredIndexPartReader: ", ex);
    }
    return componentReader;
  }
}
