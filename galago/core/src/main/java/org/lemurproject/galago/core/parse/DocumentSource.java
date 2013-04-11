// BSD License (http://lemurproject.org/galago-license)
package org.lemurproject.galago.core.parse;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.HashSet;
import java.util.logging.Logger;
import java.util.zip.GZIPInputStream;
import org.lemurproject.galago.tupleflow.execution.ErrorHandler;
import org.lemurproject.galago.tupleflow.execution.Verified;
import org.lemurproject.galago.core.types.DocumentSplit;
import org.lemurproject.galago.tupleflow.*;

/**
 * From a set of inputs, splits the input into many DocumentSplit records. This
 * will usually be in a stage by itself at the beginning of a Galago pipeline.
 * This is somewhat similar to FileSource, except that it can autodetect file
 * formats. This splitter can detect ARC, TREC, TRECWEB and corpus files.
 *
 * @author trevor, sjh, irmarc
 */
@Verified
@OutputClass(className = "org.lemurproject.galago.core.types.DocumentSplit")
public class DocumentSource implements ExNihiloSource<DocumentSplit> {

  static String[][] specialKnownExtensions = {
    {"_mbtei.xml.gz", "mbtei"}
  };
  private Counter inputCounter;
  public Processor<DocumentSplit> processor;
  private Parameters parameters;
  private int fileId = 0;
  private Set<String> externalFileTypes;
  private String forceFileType;
  private Logger logger;
  private String inputPolicy;

  public DocumentSource(String... sources) {
    this.parameters = new Parameters();
    ArrayList<String> dirs = new ArrayList<String>();
    parameters.set("directory", dirs);
    ArrayList<String> files = new ArrayList<String>();
    parameters.set("filename", files);
    for (String path : sources) {
      File f = new File(path);
      if (f.isDirectory()) {
        dirs.add(path);
      } else if (f.isFile()) {
        files.add(path);
      }
    }
    initialize();
  }

  public DocumentSource(Parameters p) {
    this.parameters = p;
    initialize();
  }

  public DocumentSource(TupleFlowParameters tfParameters) {
    this.parameters = tfParameters.getJSON();
    this.inputCounter = tfParameters.getCounter("Inputs Processed");
    initialize();
  }

  private void initialize() {
    inputPolicy = parameters.get("inputPolicy", "require");
    logger = Logger.getLogger("DOCSOURCE");
    externalFileTypes = new HashSet<String>();
    forceFileType = parameters.get("filetype", (String) null);
    if (parameters.containsKey("externalParsers")) {
      List<Parameters> extP = parameters.getAsList("externalParsers");
      for (Parameters p : extP) {
        logger.info(String.format("Adding external file type %s\n",
                p.getString("filetype")));
        externalFileTypes.add(p.getString("filetype"));
      }
    }
  }

  @Override
  public void run() throws IOException {
    if (parameters.containsKey("directory")) {
      List<String> directories = parameters.getAsList("directory");
      for (String directory : directories) {
        File directoryFile = new File(directory);
        processDirectory(directoryFile);
      }
    }
    if (parameters.containsKey("filename")) {
	List<String> files = parameters.getAsList("filename");
      for (String file : files) {
        processFile(new File(file));
      }
    }

    // All files processed.
    processor.close();
  }

  /// PRIVATE FUNCTIONS ///
  private void processDirectory(File root) throws IOException {
    System.out.println("Processing directory: " + root);
    File[] subs = root.listFiles();
    int count = 0;
    while (subs == null && count < 100) {
      try {
        Thread.sleep(1000);
      } catch (Exception e) {
      }
      System.out.println("sleeping. subs is null. Wuh?");
      count++;
      subs = root.listFiles();
    }

    if (subs != null) {
      for (File file : subs) {
        if (file.isHidden()) {
          continue;
        }
        if (file.isDirectory()) {
          processDirectory(file);
        } else {
          processFile(file);
        }
      }
    } else {
      System.out.println("subs is still null... ");
      throw new IllegalStateException("subs is null");
    }
  }

  private void processFile(File file) throws IOException {

    // First, make sure this file exists. If not, whine about it.
    if (!file.exists()) {
      if (inputPolicy.equals("require")) {
        throw new IOException(String.format("File %s was not found. Exiting.\n", file));
      } else if (inputPolicy.equals("warn")) {
        logger.warning(String.format("File %s was not found. Skipping.\n", file));
        return;
      } else {
        // Return quietly
        return;
      }
    }

    // Now try to detect what kind of file this is:
    boolean isCompressed = (file.getName().endsWith(".gz") || file.getName().endsWith(".bz2") || file.getName().endsWith(".xz"));
    String fileType = forceFileType;

    // We'll try to detect by extension first, so we don't have to open the file
    String extension = null;
    if (fileType == null) {
      extension = getExtension(file);

      // first lets look for special cases that require some processing here:
      if (extension.equals("list")) {
        processListFile(file);
        return; // now considered processed1
      }

      if (ParserSelector.isParsable(extension)) {
        fileType = extension;

      } else {
        // finally try to be 'clever'...
        fileType = detectTrecTextOrWeb(file);
      }
    }

    if (forceFileType != null) {
      fileType = forceFileType;
    } else if (ParserSelector.isParsable(extension) || isExternallyDefined(extension)) {
      fileType = extension;
    } else {
      fileType = detectTrecTextOrWeb(file);
    }
    // Eventually it'd be nice to do more format detection here.

    if (fileType != null) {
      DocumentSplit split =
	  new DocumentSplit(file.getAbsolutePath(),
			    fileType,
			    isCompressed,
			    0,
			    0);
      processor.process(split);
      ++fileId;
    } else {
	String msg =
	    String.format("Unable to determine file type of %s\n",
			  file.toString());
	if (inputPolicy.equals("require")) {
	    throw new RuntimeException(msg);
	} else if (inputPolicy.equals("warn")) {
	    logger.warning(msg);
	}
    }
  }

  /**
   * This is a list file, meaning we need to iterate over its contents to
   * retrieve the file list.
   *
   * Assumptions: Each line in this file should be a filename, NOT a directory.
   * List file is either uncompressed or compressed using gzip ONLY.
   */
  private void processListFile(File file) throws IOException {
    BufferedReader br;
    if (file.getName().endsWith("gz")) {
      br = new BufferedReader(new InputStreamReader(new GZIPInputStream(new FileInputStream(file))));
    } else {
      br = new BufferedReader(new FileReader(file));
    }

    while (br.ready()) {
      String entry = br.readLine().trim();
      if (entry.length() == 0) {
        continue;
      }
      processFile(new File(entry));
    }
    br.close();
    // No more to do here -- this file is now "processed"
  }

  private String getExtension(File file) {

    String fileName = file.getName();

    // There's confusion b/c of the naming scheme for MBTEI - so define
    // a pattern look for that before we do rule-based stuff.
    for (String[] pattern : specialKnownExtensions) {
      if (fileName.contains(pattern[0])) {
        return pattern[1];
      }
    }

    // now split the filename on '.'s
    String[] fields = fileName.split("\\.");

    // A filename needs to have a period to have an extension.
    if (fields.length <= 1) {
      return "";
    }

    // If the last chunk of the filename is gz, we'll ignore it.
    // The second-to-last bit is the type extension (but only if
    // there are at least three parts to the name).
    if (fields[fields.length - 1].equals("gz")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // Do the same thing w/ bz2 as above (MAC)
    if (fields[fields.length - 1].equals("bz2")) {
      if (fields.length > 2) {
        return fields[fields.length - 2];
      } else {
        return "";
      }
    }

    // No 'gz'/'bz2' extensions, so just return the last part.
    return fields[fields.length - 1];
  }

  // For now we assume <doc> tags, so we read in one doc
  // (i.e. <doc> to </doc>), and look for the following
  // tags: <docno> and (<text> or <html>)
  private String detectTrecTextOrWeb(File file) {
    String fileType = null;
    BufferedReader br = null;
    try {
      br = new BufferedReader(new FileReader(file));
      String line;

      // check the first line for a "<doc>" line
      line = br.readLine();
      if (line == null || line.equalsIgnoreCase("<doc>") == false) {
        return fileType;
      }

      // Now just read until we see docno and (text or html) tags
      boolean hasDocno, hasDocHdr, hasHtml, hasText, hasBody;
      hasDocno = hasDocHdr = hasHtml = hasText = hasBody = false;
      while (br.ready()) {
        line = br.readLine();
        if (line == null || line.equalsIgnoreCase("</doc>")) {
          break; // doc is closed or null line
        }
        line = line.toLowerCase();
        if (line.indexOf("<docno>") != -1) {
          hasDocno = true;
        } else if (line.indexOf("<dochdr>") != -1) {
          hasDocHdr = true;
        } else if (line.indexOf("<text>") != -1) {
          hasText = true;
        } else if (line.indexOf("<html>") != -1) {
          hasHtml = true;
        } else if (line.indexOf("<body>") != -1) {
          hasBody = true;
        }

        if (hasDocno && hasText) {
          fileType = "trectext";
          break;
        } else if (hasDocno && (hasHtml || hasBody || hasDocHdr)) {
          fileType = "trecweb";
        }
      }
      br.close();
      if (fileType != null) {
        System.out.println(file.getAbsolutePath() + " detected as " + fileType);
      } else {
        System.out.println("Unable to determine file type of " + file.getAbsolutePath());
      }
      return fileType;
    } catch (IOException ioe) {
      ioe.printStackTrace(System.err);
      return null;
    } finally {
      try {
        if (br != null) {
          br.close();
        }
      } catch (Exception e) {
      }
    }
  }

  protected boolean isExternallyDefined(String extension) {
    return externalFileTypes.contains(extension);
  }

  @Override
  public Step setProcessor(Step processor) throws IncompatibleProcessorException {
    Linkage.link(this, processor);
    return processor;
  }

  public static void verify(TupleFlowParameters parameters, ErrorHandler handler) {
    FileSource.verify(parameters, handler);
  }
}
