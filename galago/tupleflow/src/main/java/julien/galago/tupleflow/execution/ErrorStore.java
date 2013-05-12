// BSD License (http://lemurproject.org/galago-license)
package julien.galago.tupleflow.execution;

import java.util.ArrayList;
import java.util.Collections;
import org.xml.sax.SAXParseException;

/**
 *
 * @author trevor
 */
public class ErrorStore {
    public static class Statement implements Comparable<Statement> {
        public Statement(String message) {
            this.message = message;
        }

        public String toString(String messageType) {
            String result;
	    result = String.format("%s: %s\n", messageType,
				   message);
            return result;
        }

        public String toString() {
            return toString("INFO");
        }

        public int compareTo(Statement other) {
	    return 0;
        }
        String message;
    }

    ArrayList<Statement> errors = new ArrayList();
    ArrayList<Statement> warnings = new ArrayList();

    public void addError(String message) {
        errors.add(new Statement(message));
    }

    public void addWarning(String message) {
        warnings.add(new Statement(message));
    }

    public ArrayList<Statement> getErrors() {
        return errors;
    }

    public ArrayList<Statement> getWarnings() {
        return warnings;
    }

    public boolean hasStatements() {
        return errors.size() + warnings.size() > 0;
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        Collections.sort(errors);
        Collections.sort(warnings);

        for (Statement s : errors) {
            builder.append(s.toString("Error"));
        }

        for (Statement s : warnings) {
            builder.append(s.toString("Warning"));
        }

        return builder.toString();
    }
}
