// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index.disk;

import org.lemurproject.galago.core.index.NullExtentIterator;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.lemurproject.galago.core.index.AggregateReader.AggregateIndexPart;
import org.lemurproject.galago.core.index.AggregateReader.IndexPartStatistics;
import org.lemurproject.galago.core.index.BTreeFactory;
import org.lemurproject.galago.core.index.BTreeReader;
import org.lemurproject.galago.core.index.Index;
import org.lemurproject.galago.core.index.NamesReader;
import org.lemurproject.galago.core.index.IndexPartReader;
import org.lemurproject.galago.core.index.Iterator;
import org.lemurproject.galago.core.index.LengthsReader;
import org.lemurproject.galago.core.index.LengthsReader.LengthsIterator;
import org.lemurproject.galago.core.index.corpus.CorpusReader;
import org.lemurproject.galago.core.index.corpus.SplitBTreeReader;
import org.lemurproject.galago.core.parse.Document;
import org.lemurproject.galago.tupleflow.Parameters;

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

  protected File location;
  protected Parameters manifest = new Parameters();
  protected LengthsReader lengthsReader = null;
  protected NamesReader namesReader = null;
  protected Map<String, IndexPartReader> parts = new HashMap<String, IndexPartReader>();

  // useful to assemble an index from odd pieces
  public DiskIndex(Collection<String> indexParts) throws IOException {
    location = null;

    for (String indexPart : indexParts) {
      File part = new File(indexPart);
      IndexComponentReader component = openIndexComponent(part.getAbsolutePath());
      initializeComponent(part.getName(), component);
    }
    // Initialize these now b/c they're so common
    if (parts.containsKey("lengths")) {
      lengthsReader = (DiskLengthsReader) parts.get("lengths");
    }
    if (parts.containsKey("names")) {
      namesReader = (DiskNameReader) parts.get("names");
    }
  }

  public DiskIndex(String indexPath) throws IOException {
    // Make sure it's a valid location
    location = new File(indexPath);
    if (!location.isDirectory()) {
      throw new IOException(String.format("%s is not a directory.", indexPath));
    }

    // Load all parts
    openDiskParts("", location);

    // Initialize these now b/c they're so common
    if (parts.containsKey("lengths")) {
      lengthsReader = (DiskLengthsReader) parts.get("lengths");
    }
    if (parts.containsKey("names")) {
      namesReader = (DiskNameReader) parts.get("names");
    }
  }

  /**
   * recursively open index parts + infer if the file/folder is a part or a
   * modifier
   *
   * prefix should be empty string or a path ending with a slash
   */
  private void openDiskParts(String name, File directory) throws IOException {
    // check if the directory is a split index folder: (e.g. corpus)
    if (SplitBTreeReader.isBTree(directory)) {
      IndexComponentReader component = openIndexComponent(directory.getAbsolutePath());
      if (component != null) {
        initializeComponent(name, component);
      }
      return;
    }

    // otherwise the directory might contain stand-alone index files
    for (File part : directory.listFiles()) {
      String partName = (name.length() == 0) ? part.getName() : name + "/" + part.getName();
      if (part.isDirectory()) {
        openDiskParts(partName, part);
      } else {
        IndexComponentReader component = openIndexComponent(part.getAbsolutePath());
        if (component != null) {
          initializeComponent(partName, component);
        }
      }
    }
  }

  private void initializeComponent(String name, IndexComponentReader component) {
    if (IndexPartReader.class.isAssignableFrom(component.getClass())) {
      parts.put(name, (IndexPartReader) component);
    }
  }

  public File getIndexLocation() {
    return location;
  }

  public static String getPartPath(String index, String part) {
    return (index + File.separator + part);
  }

  @Override
  public IndexPartReader getIndexPart(String part) throws IOException {
    if (parts.containsKey(part)) {
      return parts.get(part);
    } else {
      return null;
    }
  }

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
  public boolean containsIdentifier(int document) throws IOException {
    NamesReader.NamesIterator ni = this.getNamesIterator();
    ni.syncTo(document);
    return ni.hasMatch(document);
  }

  @Override
  public Iterator getIterator(byte[] key, Parameters p) throws IOException {
    Iterator result = null;
    IndexPartReader part = parts.get(p.get("part", "postings"));
    if (part != null) {
      result = part.getIterator(key);
      if (result == null) {
        result = new NullExtentIterator(key);
      }
    }
    return result;
  }

  @Override
  public IndexPartStatistics getIndexPartStatistics(String part) {
    if (parts.containsKey(part)) {
      IndexPartReader p = parts.get(part);
      if (AggregateIndexPart.class.isInstance(p)) {
        return ((AggregateIndexPart) p).getStatistics();
      }
      throw new IllegalArgumentException("Index part, " + part + ", does not store aggregated statistics.");
    }
    throw new IllegalArgumentException("Index part, " + part + ", could not be found in index, " + this.location.getAbsolutePath());
  }

  @Override
  public void close() throws IOException {
    for (IndexPartReader part : parts.values()) {
      part.close();
    }
    parts.clear();
    lengthsReader.close();
    namesReader.close();
  }

  @Override
  public int getLength(int document) throws IOException {
    return lengthsReader.getLength(document);
  }

  @Override
  public String getName(int document) throws IOException {
    return namesReader.getDocumentName(document);
  }

  @Override
  public int getIdentifier(String document) throws IOException {
    return ((NamesReader) parts.get("names.reverse")).getDocumentIdentifier(document);
  }

  @Override
  public Document getItem(String name, Parameters p) throws IOException {
    if (parts.containsKey("corpus")) {
      try {
        CorpusReader corpus = (CorpusReader) parts.get("corpus");
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
    return lengthsReader.getLengthsIterator();
  }

  @Override
  public NamesReader.NamesIterator getNamesIterator() throws IOException {
    return namesReader.getNamesIterator();
  }

  @Override
  public Parameters getManifest() {
    return manifest.clone();
  }

  @Override
  public Set<String> getPartNames() {
    return parts.keySet();
  }


  /* static functions for opening index component readers */
  public static IndexComponentReader openIndexComponent(String path) throws IOException {
    BTreeReader reader = BTreeFactory.getBTreeReader(path);

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

    String className = reader.getManifest().get("readerClass", (String) null);
    Class readerClass;
    try {
      readerClass = Class.forName(className);
    } catch (ClassNotFoundException e) {
      throw new IOException("Class " + className + ", which was specified as the readerClass "
              + "in " + path + ", could not be found.");
    }

    if (!IndexComponentReader.class.isAssignableFrom(readerClass)) {
      throw new IOException(className + " is not a IndexComponentReader subclass.");
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

    IndexComponentReader componentReader;
    try {
      componentReader = (IndexComponentReader) c.newInstance(reader);
    } catch (Exception ex) {
      throw new IOException("Caught an exception while instantiating "
              + "a StructuredIndexPartReader: ", ex);
    }
    return componentReader;
  }

  public static IndexPartReader openIndexPart(String path) throws IOException {
    IndexComponentReader componentReader = openIndexComponent(path);
    if (!IndexPartReader.class.isAssignableFrom(componentReader.getClass())) {
      throw new IOException(componentReader.getClass().getName() + " is not a IndexPartReader subclass.");
    }
    return (IndexPartReader) componentReader;
  }
}
