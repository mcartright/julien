// BSD License (http://lemurproject.org/galago-license)
package julien.galago.tupleflow.execution;

import java.io.Serializable;

import julien.galago.tupleflow.Parameters;

/**
 *
 * @author trevor
 */
public class Step implements Serializable {

  private String className;
  private Parameters parameters;

  public Step() {
  }

  public Step(Class c) {
    this(c.getName(), new Parameters());
  }

  public Step(String className) {
    this(className, new Parameters());
  }

  public Step(Class c, Parameters parameters) {
    this(c.getName(), parameters);
  }

  public Step(String className, Parameters parameters) {
    this.className = className;
    this.parameters = parameters;
  }

  public String getClassName() {
    return className;
  }

  public Parameters getParameters() {
    return parameters;
  }

  public boolean isStepClassAvailable() {
    return Verification.isClassAvailable(className);
  }

  @Override
  public String toString() {
    return className;
  }
}
