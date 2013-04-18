// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.index;

import java.io.IOException;
import org.lemurproject.galago.core.types.KeyValuePair;
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.Parameters;
import org.lemurproject.galago.tupleflow.Source;
import org.lemurproject.galago.tupleflow.Step;

/**
 * This class writes index files, which are used for most Galago indexes.
 *
 * An index is a mapping between a key and a value, much like a TreeMap.  The keys are
 * sorted to allow iteration over the whole file.  Keys are stored using prefix
 * compression to save space.  The structure is designed for fast random access on disk.
 *
 * For indexes, we assume that the data in each value is already compressed, so IndexWriter
 * does no additional compression.
 *
 * An IndexWriter is a special case, it can compress the data if isCompressed is set.
 *
 * Keys cannot be longer than 256 bytes, and they must be added in sorted order.
 *
 * There are two ways of adding data to an index:
 *
 *  - add
 *      - adds the entire key-value pair in a single wrapper
 *      - this type may be stored partially on disk (see GenericElement)
 *
 *  - processKey/Tuple
 *      - adds a key and several value blocks
 *      - this tuple allows for partial value data to be written
 *        to the index as it is generated
 *      - both key and value blocks must be explicitly represented in memory
 *      - this produces some memory limitations
 *
 * @author sjh
 */
public interface BTreeWriter {

    /**
     * Returns the current copy of the manifest.
     */
    public abstract Parameters getManifest();

    /**
     * Adds a key-value pair of byte[]s to the index. No size requirement for
     * either the key or value is imposed at this level.
     */
    public abstract void add(IndexElement list) throws IOException ;

    /**
     * Closes the writer and finalizes index structure. Adding new IndexElement
     * objects after this call should result in an Exception, as the index
     * structure has already been finalized.
     */
    public abstract void close() throws IOException;
}
