// This file was automatically generated with the command: 
//     java org.lemurproject.galago.tupleflow.typebuilder.TypeBuilderMojo ...
package org.lemurproject.galago.tupleflow.types;

import org.lemurproject.galago.tupleflow.Utility;
import org.lemurproject.galago.tupleflow.ArrayInput;
import org.lemurproject.galago.tupleflow.ArrayOutput;
import org.lemurproject.galago.tupleflow.Order;   
import org.lemurproject.galago.tupleflow.OrderedWriter;
import org.lemurproject.galago.tupleflow.Type; 
import org.lemurproject.galago.tupleflow.TypeReader;
import org.lemurproject.galago.tupleflow.Step; 
import org.lemurproject.galago.tupleflow.IncompatibleProcessorException;
import org.lemurproject.galago.tupleflow.ReaderSource;
import java.io.IOException;             
import java.io.EOFException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.Arrays;   
import java.util.Comparator;
import java.util.PriorityQueue;
import java.util.Collection;
import gnu.trove.list.array.TByteArrayList;
import gnu.trove.list.array.TDoubleArrayList;
import gnu.trove.list.array.TFloatArrayList;
import gnu.trove.list.array.TIntArrayList;
import gnu.trove.list.array.TLongArrayList;
import gnu.trove.list.array.TShortArrayList;


public class TupleflowString implements Type<TupleflowString> {
    public String value; 
    
    public TupleflowString() {}
    public TupleflowString(String value) {
        this.value = value;
    }  
    
    public String toString() {
            return String.format("%s",
                                   value);
    } 

    public Order<TupleflowString> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+value" })) {
            return new ValueOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, org.lemurproject.galago.tupleflow.Processor<TupleflowString> {
        public void process(TupleflowString object) throws IOException;
        public void close() throws IOException;
    }                        
    public interface Source extends Step {
    }
    public static class ValueOrder implements Order<TupleflowString> {
        public int hash(TupleflowString object) {
            int h = 0;
            h += Utility.hash(object.value);
            return h;
        } 
        public Comparator<TupleflowString> greaterThan() {
            return new Comparator<TupleflowString>() {
                public int compare(TupleflowString one, TupleflowString two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.value, two.value);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<TupleflowString> lessThan() {
            return new Comparator<TupleflowString>() {
                public int compare(TupleflowString one, TupleflowString two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.value, two.value);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<TupleflowString> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<TupleflowString> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<TupleflowString> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< TupleflowString > {
            TupleflowString last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(TupleflowString object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != Utility.compare(object.value, last.value)) { processAll = true; shreddedWriter.processValue(object.value); }
               shreddedWriter.processTuple();
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<TupleflowString> getInputClass() {
                return TupleflowString.class;
            }
        } 
        public ReaderSource<TupleflowString> orderedCombiner(Collection<TypeReader<TupleflowString>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<TupleflowString> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public TupleflowString clone(TupleflowString object) {
            TupleflowString result = new TupleflowString();
            if (object == null) return result;
            result.value = object.value; 
            return result;
        }                 
        public Class<TupleflowString> getOrderedClass() {
            return TupleflowString.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+value"};
        }

        public static String[] getSpec() {
            return new String[] {"+value"};
        }
        public static String getSpecString() {
            return "+value";
        }
                           
        public interface ShreddedProcessor extends Step {
            public void processValue(String value) throws IOException;
            public void processTuple() throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastValue;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public void processValue(String value) {
                lastValue = value;
                buffer.processValue(value);
            }
            public final void processTuple() throws IOException {
                if (lastFlush) {
                    if(buffer.values.size() == 0) buffer.processValue(lastValue);
                    lastFlush = false;
                }
                buffer.processTuple();
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    buffer.incrementTuple();
                }
            }  
            public final void flushValue(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getValueEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getValue());
                    output.writeInt(count);
                    buffer.incrementValue();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushValue(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static class ShreddedBuffer {
            ArrayList<String> values = new ArrayList();
            TIntArrayList valueTupleIdx = new TIntArrayList();
            int valueReadIdx = 0;
                            
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processValue(String value) {
                values.add(value);
                valueTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple() {
                assert values.size() > 0;
                writeTupleIndex++;
            }
            public void resetData() {
                values.clear();
                valueTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                valueReadIdx = 0;
            } 

            public void reset() {
                resetData();
                resetRead();
            } 
            public boolean isFull() {
                return writeTupleIndex >= batchSize;
            }

            public boolean isEmpty() {
                return writeTupleIndex == 0;
            }                          

            public boolean isAtEnd() {
                return readTupleIndex >= writeTupleIndex;
            }           
            public void incrementValue() {
                valueReadIdx++;  
            }                                                                                              

            public void autoIncrementValue() {
                while (readTupleIndex >= getValueEndIndex() && readTupleIndex < writeTupleIndex)
                    valueReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getValueEndIndex() {
                if ((valueReadIdx+1) >= valueTupleIdx.size())
                    return writeTupleIndex;
                return valueTupleIdx.get(valueReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getValue() {
                assert readTupleIndex < writeTupleIndex;
                assert valueReadIdx < values.size();
                
                return values.get(valueReadIdx);
            }

            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple();
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexValue(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processValue(getValue());
                    assert getValueEndIndex() <= endIndex;
                    copyTuples(getValueEndIndex(), output);
                    incrementValue();
                }
            }  
            public void copyUntilValue(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getValue(), other.getValue());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processValue(getValue());
                                      
                        copyTuples(getValueEndIndex(), output);
                    } else {
                        output.processValue(getValue());
                        copyTuples(getValueEndIndex(), output);
                    }
                    incrementValue();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilValue(other, output);
            }
            
        }                         
        public static class ShreddedCombiner implements ReaderSource<TupleflowString>, ShreddedSource {   
            public ShreddedProcessor processor;
            Collection<ShreddedReader> readers;       
            boolean closeOnExit = false;
            boolean uninitialized = true;
            PriorityQueue<ShreddedReader> queue = new PriorityQueue<ShreddedReader>();
            
            public ShreddedCombiner(Collection<ShreddedReader> readers, boolean closeOnExit) {
                this.readers = readers;                                                       
                this.closeOnExit = closeOnExit;
            }
                                  
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof TupleflowString.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((TupleflowString.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<TupleflowString>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<TupleflowString> getOutputClass() {
                return TupleflowString.class;
            }
            
            public void initialize() throws IOException {
                for (ShreddedReader reader : readers) {
                    reader.fill();                                        
                    
                    if (!reader.getBuffer().isAtEnd())
                        queue.add(reader);
                }   

                uninitialized = false;
            }

            public void run() throws IOException {
                initialize();
               
                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    ShreddedReader next = null;
                    ShreddedBuffer nextBuffer = null; 
                    
                    assert !top.getBuffer().isAtEnd();
                                                  
                    if (queue.size() > 0) {
                        next = queue.peek();
                        nextBuffer = next.getBuffer();
                        assert !nextBuffer.isAtEnd();
                    }
                    
                    top.getBuffer().copyUntil(nextBuffer, processor);
                    if (top.getBuffer().isAtEnd())
                        top.fill();                 
                        
                    if (!top.getBuffer().isAtEnd())
                        queue.add(top);
                }              
                
                if (closeOnExit)
                    processor.close();
            }

            public TupleflowString read() throws IOException {
                if (uninitialized)
                    initialize();

                TupleflowString result = null;

                while (queue.size() > 0) {
                    ShreddedReader top = queue.poll();
                    result = top.read();

                    if (result != null) {
                        if (top.getBuffer().isAtEnd())
                            top.fill();

                        queue.offer(top);
                        break;
                    } 
                }

                return result;
            }
        } 
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<TupleflowString>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            TupleflowString last = new TupleflowString();         
            long updateValueCount = -1;
            long tupleCount = 0;
            long bufferStartCount = 0;  
            ArrayInput input;
            
            public ShreddedReader(ArrayInput input) {
                this.input = input; 
                this.buffer = new ShreddedBuffer();
            }                               
            
            public ShreddedReader(ArrayInput input, int bufferSize) { 
                this.input = input;
                this.buffer = new ShreddedBuffer(bufferSize);
            }
                 
            public final int compareTo(ShreddedReader other) {
                ShreddedBuffer otherBuffer = other.getBuffer();
                
                if (buffer.isAtEnd() && otherBuffer.isAtEnd()) {
                    return 0;                 
                } else if (buffer.isAtEnd()) {
                    return -1;
                } else if (otherBuffer.isAtEnd()) {
                    return 1;
                }
                                   
                int result = 0;
                do {
                    result = + Utility.compare(buffer.getValue(), otherBuffer.getValue());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final TupleflowString read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                TupleflowString result = new TupleflowString();
                
                result.value = buffer.getValue();
                
                buffer.incrementTuple();
                buffer.autoIncrementValue();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateValueCount - tupleCount > 0) {
                            buffer.values.add(last.value);
                            buffer.valueTupleIdx.add((int) (updateValueCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateValue();
                        buffer.processTuple();
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateValue() throws IOException {
                if (updateValueCount > tupleCount)
                    return;
                     
                last.value = input.readString();
                updateValueCount = tupleCount + input.readInt();
                                      
                buffer.processValue(last.value);
            }

            public void run() throws IOException {
                while (true) {
                    fill();
                    
                    if (buffer.isAtEnd())
                        break;
                    
                    buffer.copyUntil(null, processor);
                }      
                processor.close();
            }
            
            public void setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof TupleflowString.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((TupleflowString.Processor) processor));
                } else if (processor instanceof org.lemurproject.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((org.lemurproject.galago.tupleflow.Processor<TupleflowString>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
            }                                
            
            public Class<TupleflowString> getOutputClass() {
                return TupleflowString.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            TupleflowString last = new TupleflowString();
            boolean valueProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processValue(String value) throws IOException {  
                if (valueProcess || Utility.compare(value, last.value) != 0) {
                    last.value = value;
                    processor.processValue(value);
                    valueProcess = false;
                }
            }  
            
            public void resetValue() {
                 valueProcess = true;
            }                                                
                               
            public void processTuple() throws IOException {
                processor.processTuple();
            } 
            
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static class TupleUnshredder implements ShreddedProcessor {
            TupleflowString last = new TupleflowString();
            public org.lemurproject.galago.tupleflow.Processor<TupleflowString> processor;                               
            
            public TupleUnshredder(TupleflowString.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(org.lemurproject.galago.tupleflow.Processor<TupleflowString> processor) {
                this.processor = processor;
            }
            
            public TupleflowString clone(TupleflowString object) {
                TupleflowString result = new TupleflowString();
                if (object == null) return result;
                result.value = object.value; 
                return result;
            }                 
            
            public void processValue(String value) throws IOException {
                last.value = value;
            }   
                
            
            public void processTuple() throws IOException {
                processor.process(clone(last));
            }               
            
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static class TupleShredder implements Processor {
            TupleflowString last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public TupleflowString clone(TupleflowString object) {
                TupleflowString result = new TupleflowString();
                if (object == null) return result;
                result.value = object.value; 
                return result;
            }                 
            
            public void process(TupleflowString object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || Utility.compare(last.value, object.value) != 0 || processAll) { processor.processValue(object.value); processAll = true; }
                processor.processTuple();                                         
                last = object;
            }
                          
            public Class<TupleflowString> getInputClass() {
                return TupleflowString.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    