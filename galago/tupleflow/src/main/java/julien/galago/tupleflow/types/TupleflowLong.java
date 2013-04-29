// This file was automatically generated with the command: 
//     java julien.galago.tupleflow.typebuilder.TypeBuilderMojo ...
package julien.galago.tupleflow.types;

import julien.galago.tupleflow.Utility;
import julien.galago.tupleflow.ArrayInput;
import julien.galago.tupleflow.ArrayOutput;
import julien.galago.tupleflow.Order;   
import julien.galago.tupleflow.OrderedWriter;
import julien.galago.tupleflow.Type; 
import julien.galago.tupleflow.TypeReader;
import julien.galago.tupleflow.Step; 
import julien.galago.tupleflow.IncompatibleProcessorException;
import julien.galago.tupleflow.ReaderSource;
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


public class TupleflowLong implements Type<TupleflowLong> {
    public long value; 
    
    public TupleflowLong() {}
    public TupleflowLong(long value) {
        this.value = value;
    }  
    
    public String toString() {
            return String.format("%d",
                                   value);
    } 

    public Order<TupleflowLong> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+value" })) {
            return new ValueOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, julien.galago.tupleflow.Processor<TupleflowLong> {
        public void process(TupleflowLong object) throws IOException;
        public void close() throws IOException;
    }                        
    public interface Source extends Step {
    }
    public static class ValueOrder implements Order<TupleflowLong> {
        public int hash(TupleflowLong object) {
            int h = 0;
            h += Utility.hash(object.value);
            return h;
        } 
        public Comparator<TupleflowLong> greaterThan() {
            return new Comparator<TupleflowLong>() {
                public int compare(TupleflowLong one, TupleflowLong two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.value, two.value);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<TupleflowLong> lessThan() {
            return new Comparator<TupleflowLong>() {
                public int compare(TupleflowLong one, TupleflowLong two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.value, two.value);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<TupleflowLong> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<TupleflowLong> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<TupleflowLong> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< TupleflowLong > {
            TupleflowLong last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(TupleflowLong object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != Utility.compare(object.value, last.value)) { processAll = true; shreddedWriter.processValue(object.value); }
               shreddedWriter.processTuple();
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<TupleflowLong> getInputClass() {
                return TupleflowLong.class;
            }
        } 
        public ReaderSource<TupleflowLong> orderedCombiner(Collection<TypeReader<TupleflowLong>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<TupleflowLong> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public TupleflowLong clone(TupleflowLong object) {
            TupleflowLong result = new TupleflowLong();
            if (object == null) return result;
            result.value = object.value; 
            return result;
        }                 
        public Class<TupleflowLong> getOrderedClass() {
            return TupleflowLong.class;
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
            public void processValue(long value) throws IOException;
            public void processTuple() throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            long lastValue;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public void processValue(long value) {
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
                    
                    output.writeLong(buffer.getValue());
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
            TLongArrayList values = new TLongArrayList();
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
            
            public void processValue(long value) {
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
            public long getValue() {
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
        public static class ShreddedCombiner implements ReaderSource<TupleflowLong>, ShreddedSource {   
            public ShreddedProcessor processor;
            Collection<ShreddedReader> readers;       
            boolean closeOnExit = false;
            boolean uninitialized = true;
            PriorityQueue<ShreddedReader> queue = new PriorityQueue<ShreddedReader>();
            
            public ShreddedCombiner(Collection<ShreddedReader> readers, boolean closeOnExit) {
                this.readers = readers;                                                       
                this.closeOnExit = closeOnExit;
            }
                                  
            public Step setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof TupleflowLong.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((TupleflowLong.Processor) processor));
                } else if (processor instanceof julien.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((julien.galago.tupleflow.Processor<TupleflowLong>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
                return processor;
            }                                
            
            public Class<TupleflowLong> getOutputClass() {
                return TupleflowLong.class;
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

            public TupleflowLong read() throws IOException {
                if (uninitialized)
                    initialize();

                TupleflowLong result = null;

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
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<TupleflowLong>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            TupleflowLong last = new TupleflowLong();         
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
            
            public final TupleflowLong read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                TupleflowLong result = new TupleflowLong();
                
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
                     
                last.value = input.readLong();
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
            
            public Step setProcessor(Step processor) throws IncompatibleProcessorException {  
                if (processor instanceof ShreddedProcessor) {
                    this.processor = new DuplicateEliminator((ShreddedProcessor) processor);
                } else if (processor instanceof TupleflowLong.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((TupleflowLong.Processor) processor));
                } else if (processor instanceof julien.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((julien.galago.tupleflow.Processor<TupleflowLong>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
                return processor;
            }                                
            
            public Class<TupleflowLong> getOutputClass() {
                return TupleflowLong.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            TupleflowLong last = new TupleflowLong();
            boolean valueProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processValue(long value) throws IOException {  
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
            TupleflowLong last = new TupleflowLong();
            public julien.galago.tupleflow.Processor<TupleflowLong> processor;                               
            
            public TupleUnshredder(TupleflowLong.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(julien.galago.tupleflow.Processor<TupleflowLong> processor) {
                this.processor = processor;
            }
            
            public TupleflowLong clone(TupleflowLong object) {
                TupleflowLong result = new TupleflowLong();
                if (object == null) return result;
                result.value = object.value; 
                return result;
            }                 
            
            public void processValue(long value) throws IOException {
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
            TupleflowLong last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public TupleflowLong clone(TupleflowLong object) {
                TupleflowLong result = new TupleflowLong();
                if (object == null) return result;
                result.value = object.value; 
                return result;
            }                 
            
            public void process(TupleflowLong object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || Utility.compare(last.value, object.value) != 0 || processAll) { processor.processValue(object.value); processAll = true; }
                processor.processTuple();                                         
                last = object;
            }
                          
            public Class<TupleflowLong> getInputClass() {
                return TupleflowLong.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    