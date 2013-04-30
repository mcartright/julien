/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package julien.galago.tupleflow;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;   
import java.util.Comparator;
import java.util.Collection;

import julien.galago.tupleflow.ArrayInput;
import julien.galago.tupleflow.ArrayOutput;
import julien.galago.tupleflow.Order;
import julien.galago.tupleflow.Processor;
import julien.galago.tupleflow.ReaderSource;
import julien.galago.tupleflow.Type;
import julien.galago.tupleflow.TypeReader;
import julien.galago.tupleflow.Utility;

/**
 *
 * @author trevor
 */
public class FakeType implements Type {
    public int value;
    
    public Order<FakeType> getOrder( String... fields ) {
        return new Order<FakeType>() {
            public Class<FakeType> getOrderedClass() {
                return FakeType.class;
            }

            public String[] getOrderSpec() {
                return new String[] { "+value" };
            }
    
            public Comparator<FakeType> lessThan() {
                return new Comparator<FakeType>() {
                    public int compare(FakeType one, FakeType two) {
                        return Utility.compare(one.value, two.value);
                    }
                };
            }
            
            public Comparator<FakeType> greaterThan() {
                return new Comparator<FakeType>() {
                    public int compare(FakeType one, FakeType two) {
                        return -Utility.compare(one.value, two.value);
                    }
                };
            }
            
            public int hash( FakeType object ) {
                return object.value;
            }

            public Processor<FakeType> orderedWriter( final ArrayOutput output ) {
                return new Processor<FakeType>() {
                    public void process( FakeType object ) throws IOException {
                        output.writeInt(object.value);
                    }
                    
                    public void close() {
                    }
                };
            }
            
            public TypeReader<FakeType> orderedReader( ArrayInput input ) {
                return null;
            }
            public TypeReader<FakeType> orderedReader( ArrayInput input, int bufferSize ) {
                return orderedReader(input);
            }
            
            public ReaderSource<FakeType> orderedCombiner( Collection< TypeReader<FakeType> > readers, boolean closeOnExit ) {
                return null;
            }
        };
    }
}
