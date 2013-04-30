// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.index;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Set;

import julien.galago.core.index.AggregateReader.CollectionStatistics;
import julien.galago.core.index.AggregateReader.IndexPartStatistics;
import julien.galago.core.index.LengthsReader.LengthsIterator;
import julien.galago.core.parse.Document;
import julien.galago.tupleflow.Parameters;


/**
 * Describes the general contract that must be fulfilled by an implementing class.
 * @author irmarc
 */
public interface Index {

  public interface IndexComponentReader {
    public Parameters getManifest();
    public void close() throws IOException;
  }

  public IndexPartReader getIndexPart(String part) throws IOException;

  public boolean containsIdentifier(int identifer) throws IOException;

  public boolean containsPart(String partName);

  public Iterator getIterator(byte[] key, Parameters p) throws IOException;

  /**
   * This contains statistics gathered for a particular field, from a
   * posting-list oriented view.
   */
  public IndexPartStatistics getIndexPartStatistics(String part);

  /**
   * This contains statistics gathered for a particular field, from a
   * document oriented view.
   */
  public CollectionStatistics getCollectionStatistics(String part);
  
  public void close() throws IOException;

  public int getLength(int identifier) throws IOException;

  public String getName(int identifier) throws IOException;

  public int getIdentifier(String name) throws IOException;

  public Document getItem(String name, Parameters p) throws IOException;

  public Map<String,Document> getItems(List<String> names, Parameters p) throws IOException;

  public LengthsIterator getLengthsIterator() throws IOException;

  public NamesReader.NamesIterator getNamesIterator() throws IOException;

  public Parameters getManifest();

  public Set<String> getPartNames();
}
