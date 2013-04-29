// BSD License (http://lemurproject.org/galago-license)
package julien.galago.core.parse;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;

import julien.galago.core.types.DocumentSplit;
import julien.galago.tupleflow.Parameters;


/**
 *
 * Twitter data parser
 *
 * assumed data format: <user> <timestamp> <tweet> <tweet-source>
 *
 * @author sjh
 */
public class TwitterParser extends DocumentStreamParser {

  BufferedReader reader;

  /**
   * Creates a new instance of TrecTextParser
   */
  public TwitterParser(DocumentSplit split, Parameters p) throws FileNotFoundException, IOException {
    super(split, p);
    this.reader = getBufferedReader(split);
  }

  public String waitFor(String tag) throws IOException {
    String line;

    while ((line = reader.readLine()) != null) {
      if (line.startsWith(tag)) {
        return line;
      }
    }

    return null;
  }

  public Document nextDocument() throws IOException {
    // entire document exists on a single line -
    String line;
    String[] data;
    // data is split by tabs

    while ((line = reader.readLine()) != null) {
      data = line.split("\t");
      if (data.length == 4) {
        String identifier = data[0] + "-" + data[1];
        String tweet = data[2];
        Document res = new Document(identifier, tweet);
        res.metadata.put("user", data[0]);
        res.metadata.put("timestamp", data[1]);
        res.metadata.put("source", data[3]);
        return res;
      } else {
        System.err.println("Error Line: " + line);
      }
    }
    return null;
  }

  // currently not used.
  public String cleanTweet(String tweet) {
    // remove user - references @xxxx
    tweet = tweet.replaceAll("@[^\\s]+", "");
    // remove urls - http://bit.ly/xxx
    tweet = tweet.replaceAll("http://[^\\s]+", "");
    return tweet;
  }

  @Override
  public void close() throws IOException {
    if (reader != null) {
      this.reader.close();
      reader = null;
    }
  }
}
