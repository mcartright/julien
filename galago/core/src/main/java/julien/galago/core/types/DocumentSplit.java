// This file was automatically generated with the command: 
//     java julien.galago.tupleflow.typebuilder.TypeBuilderMojo ...
package julien.galago.core.types;

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


public class DocumentSplit implements Type<DocumentSplit> {
    public String fileName;
    public String fileType;
    public boolean isCompressed;
    public int numDocuments;
    public int startDocument; 
    
    public DocumentSplit() {}
    public DocumentSplit(String fileName, String fileType, boolean isCompressed, int numDocuments, int startDocument) {
        this.fileName = fileName;
        this.fileType = fileType;
        this.isCompressed = isCompressed;
        this.numDocuments = numDocuments;
        this.startDocument = startDocument;
    }  
    
    public String toString() {
            return String.format("%s,%s,%b,%d,%d",
                                   fileName, fileType, isCompressed, numDocuments, startDocument);
    } 

    public Order<DocumentSplit> getOrder(String... spec) {
        if (Arrays.equals(spec, new String[] { "+fileName" })) {
            return new FileNameOrder();
        }
        return null;
    } 
      
    public interface Processor extends Step, julien.galago.tupleflow.Processor<DocumentSplit> {
        public void process(DocumentSplit object) throws IOException;
        public void close() throws IOException;
    }                        
    public interface Source extends Step {
    }
    public static class FileNameOrder implements Order<DocumentSplit> {
        public int hash(DocumentSplit object) {
            int h = 0;
            h += Utility.hash(object.fileName);
            return h;
        } 
        public Comparator<DocumentSplit> greaterThan() {
            return new Comparator<DocumentSplit>() {
                public int compare(DocumentSplit one, DocumentSplit two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.fileName, two.fileName);
                        if(result != 0) break;
                    } while (false);
                    return -result;
                }
            };
        }     
        public Comparator<DocumentSplit> lessThan() {
            return new Comparator<DocumentSplit>() {
                public int compare(DocumentSplit one, DocumentSplit two) {
                    int result = 0;
                    do {
                        result = + Utility.compare(one.fileName, two.fileName);
                        if(result != 0) break;
                    } while (false);
                    return result;
                }
            };
        }     
        public TypeReader<DocumentSplit> orderedReader(ArrayInput _input) {
            return new ShreddedReader(_input);
        }    

        public TypeReader<DocumentSplit> orderedReader(ArrayInput _input, int bufferSize) {
            return new ShreddedReader(_input, bufferSize);
        }    
        public OrderedWriter<DocumentSplit> orderedWriter(ArrayOutput _output) {
            ShreddedWriter w = new ShreddedWriter(_output);
            return new OrderedWriterClass(w); 
        }                                    
        public static class OrderedWriterClass extends OrderedWriter< DocumentSplit > {
            DocumentSplit last = null;
            ShreddedWriter shreddedWriter = null; 
            
            public OrderedWriterClass(ShreddedWriter s) {
                this.shreddedWriter = s;
            }
            
            public void process(DocumentSplit object) throws IOException {
               boolean processAll = false;
               if (processAll || last == null || 0 != Utility.compare(object.fileName, last.fileName)) { processAll = true; shreddedWriter.processFileName(object.fileName); }
               shreddedWriter.processTuple(object.fileType, object.isCompressed, object.numDocuments, object.startDocument);
               last = object;
            }           
                 
            public void close() throws IOException {
                shreddedWriter.close();
            }
            
            public Class<DocumentSplit> getInputClass() {
                return DocumentSplit.class;
            }
        } 
        public ReaderSource<DocumentSplit> orderedCombiner(Collection<TypeReader<DocumentSplit>> readers, boolean closeOnExit) {
            ArrayList<ShreddedReader> shreddedReaders = new ArrayList();
            
            for (TypeReader<DocumentSplit> reader : readers) {
                shreddedReaders.add((ShreddedReader)reader);
            }
            
            return new ShreddedCombiner(shreddedReaders, closeOnExit);
        }                  
        public DocumentSplit clone(DocumentSplit object) {
            DocumentSplit result = new DocumentSplit();
            if (object == null) return result;
            result.fileName = object.fileName; 
            result.fileType = object.fileType; 
            result.isCompressed = object.isCompressed; 
            result.numDocuments = object.numDocuments; 
            result.startDocument = object.startDocument; 
            return result;
        }                 
        public Class<DocumentSplit> getOrderedClass() {
            return DocumentSplit.class;
        }                           
        public String[] getOrderSpec() {
            return new String[] {"+fileName"};
        }

        public static String[] getSpec() {
            return new String[] {"+fileName"};
        }
        public static String getSpecString() {
            return "+fileName";
        }
                           
        public interface ShreddedProcessor extends Step {
            public void processFileName(String fileName) throws IOException;
            public void processTuple(String fileType, boolean isCompressed, int numDocuments, int startDocument) throws IOException;
            public void close() throws IOException;
        }    
        public interface ShreddedSource extends Step {
        }                                              
        
        public static class ShreddedWriter implements ShreddedProcessor {
            ArrayOutput output;
            ShreddedBuffer buffer = new ShreddedBuffer();
            String lastFileName;
            boolean lastFlush = false;
            
            public ShreddedWriter(ArrayOutput output) {
                this.output = output;
            }                        
            
            public void close() throws IOException {
                flush();
            }
            
            public void processFileName(String fileName) {
                lastFileName = fileName;
                buffer.processFileName(fileName);
            }
            public final void processTuple(String fileType, boolean isCompressed, int numDocuments, int startDocument) throws IOException {
                if (lastFlush) {
                    if(buffer.fileNames.size() == 0) buffer.processFileName(lastFileName);
                    lastFlush = false;
                }
                buffer.processTuple(fileType, isCompressed, numDocuments, startDocument);
                if (buffer.isFull())
                    flush();
            }
            public final void flushTuples(int pauseIndex) throws IOException {
                
                while (buffer.getReadIndex() < pauseIndex) {
                           
                    output.writeString(buffer.getFileType());
                    output.writeBoolean(buffer.getIsCompressed());
                    output.writeInt(buffer.getNumDocuments());
                    output.writeInt(buffer.getStartDocument());
                    buffer.incrementTuple();
                }
            }  
            public final void flushFileName(int pauseIndex) throws IOException {
                while (buffer.getReadIndex() < pauseIndex) {
                    int nextPause = buffer.getFileNameEndIndex();
                    int count = nextPause - buffer.getReadIndex();
                    
                    output.writeString(buffer.getFileName());
                    output.writeInt(count);
                    buffer.incrementFileName();
                      
                    flushTuples(nextPause);
                    assert nextPause == buffer.getReadIndex();
                }
            }
            public void flush() throws IOException { 
                flushFileName(buffer.getWriteIndex());
                buffer.reset(); 
                lastFlush = true;
            }                           
        }
        public static class ShreddedBuffer {
            ArrayList<String> fileNames = new ArrayList();
            TIntArrayList fileNameTupleIdx = new TIntArrayList();
            int fileNameReadIdx = 0;
                            
            String[] fileTypes;
            boolean[] isCompresseds;
            int[] numDocumentss;
            int[] startDocuments;
            int writeTupleIndex = 0;
            int readTupleIndex = 0;
            int batchSize;

            public ShreddedBuffer(int batchSize) {
                this.batchSize = batchSize;

                fileTypes = new String[batchSize];
                isCompresseds = new boolean[batchSize];
                numDocumentss = new int[batchSize];
                startDocuments = new int[batchSize];
            }                              

            public ShreddedBuffer() {    
                this(10000);
            }                                                                                                                    
            
            public void processFileName(String fileName) {
                fileNames.add(fileName);
                fileNameTupleIdx.add(writeTupleIndex);
            }                                      
            public void processTuple(String fileType, boolean isCompressed, int numDocuments, int startDocument) {
                assert fileNames.size() > 0;
                fileTypes[writeTupleIndex] = fileType;
                isCompresseds[writeTupleIndex] = isCompressed;
                numDocumentss[writeTupleIndex] = numDocuments;
                startDocuments[writeTupleIndex] = startDocument;
                writeTupleIndex++;
            }
            public void resetData() {
                fileNames.clear();
                fileNameTupleIdx.clear();
                writeTupleIndex = 0;
            }                  
                                 
            public void resetRead() {
                readTupleIndex = 0;
                fileNameReadIdx = 0;
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
            public void incrementFileName() {
                fileNameReadIdx++;  
            }                                                                                              

            public void autoIncrementFileName() {
                while (readTupleIndex >= getFileNameEndIndex() && readTupleIndex < writeTupleIndex)
                    fileNameReadIdx++;
            }                 
            public void incrementTuple() {
                readTupleIndex++;
            }                    
            public int getFileNameEndIndex() {
                if ((fileNameReadIdx+1) >= fileNameTupleIdx.size())
                    return writeTupleIndex;
                return fileNameTupleIdx.get(fileNameReadIdx+1);
            }
            public int getReadIndex() {
                return readTupleIndex;
            }   

            public int getWriteIndex() {
                return writeTupleIndex;
            } 
            public String getFileName() {
                assert readTupleIndex < writeTupleIndex;
                assert fileNameReadIdx < fileNames.size();
                
                return fileNames.get(fileNameReadIdx);
            }
            public String getFileType() {
                assert readTupleIndex < writeTupleIndex;
                return fileTypes[readTupleIndex];
            }                                         
            public boolean getIsCompressed() {
                assert readTupleIndex < writeTupleIndex;
                return isCompresseds[readTupleIndex];
            }                                         
            public int getNumDocuments() {
                assert readTupleIndex < writeTupleIndex;
                return numDocumentss[readTupleIndex];
            }                                         
            public int getStartDocument() {
                assert readTupleIndex < writeTupleIndex;
                return startDocuments[readTupleIndex];
            }                                         
            public void copyTuples(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                   output.processTuple(getFileType(), getIsCompressed(), getNumDocuments(), getStartDocument());
                   incrementTuple();
                }
            }                                                                           
            public void copyUntilIndexFileName(int endIndex, ShreddedProcessor output) throws IOException {
                while (getReadIndex() < endIndex) {
                    output.processFileName(getFileName());
                    assert getFileNameEndIndex() <= endIndex;
                    copyTuples(getFileNameEndIndex(), output);
                    incrementFileName();
                }
            }  
            public void copyUntilFileName(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                while (!isAtEnd()) {
                    if (other != null) {   
                        assert !other.isAtEnd();
                        int c = + Utility.compare(getFileName(), other.getFileName());
                    
                        if (c > 0) {
                            break;   
                        }
                        
                        output.processFileName(getFileName());
                                      
                        copyTuples(getFileNameEndIndex(), output);
                    } else {
                        output.processFileName(getFileName());
                        copyTuples(getFileNameEndIndex(), output);
                    }
                    incrementFileName();  
                    
               
                }
            }
            public void copyUntil(ShreddedBuffer other, ShreddedProcessor output) throws IOException {
                copyUntilFileName(other, output);
            }
            
        }                         
        public static class ShreddedCombiner implements ReaderSource<DocumentSplit>, ShreddedSource {   
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
                } else if (processor instanceof DocumentSplit.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentSplit.Processor) processor));
                } else if (processor instanceof julien.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((julien.galago.tupleflow.Processor<DocumentSplit>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
                return processor;
            }                                
            
            public Class<DocumentSplit> getOutputClass() {
                return DocumentSplit.class;
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

            public DocumentSplit read() throws IOException {
                if (uninitialized)
                    initialize();

                DocumentSplit result = null;

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
        public static class ShreddedReader implements Step, Comparable<ShreddedReader>, TypeReader<DocumentSplit>, ShreddedSource {      
            public ShreddedProcessor processor;
            ShreddedBuffer buffer;
            DocumentSplit last = new DocumentSplit();         
            long updateFileNameCount = -1;
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
                    result = + Utility.compare(buffer.getFileName(), otherBuffer.getFileName());
                    if(result != 0) break;
                } while (false);                                             
                
                return result;
            }
            
            public final ShreddedBuffer getBuffer() {
                return buffer;
            }                
            
            public final DocumentSplit read() throws IOException {
                if (buffer.isAtEnd()) {
                    fill();             
                
                    if (buffer.isAtEnd()) {
                        return null;
                    }
                }
                      
                assert !buffer.isAtEnd();
                DocumentSplit result = new DocumentSplit();
                
                result.fileName = buffer.getFileName();
                result.fileType = buffer.getFileType();
                result.isCompressed = buffer.getIsCompressed();
                result.numDocuments = buffer.getNumDocuments();
                result.startDocument = buffer.getStartDocument();
                
                buffer.incrementTuple();
                buffer.autoIncrementFileName();
                
                return result;
            }           
            
            public final void fill() throws IOException {
                try {   
                    buffer.reset();
                    
                    if (tupleCount != 0) {
                                                      
                        if(updateFileNameCount - tupleCount > 0) {
                            buffer.fileNames.add(last.fileName);
                            buffer.fileNameTupleIdx.add((int) (updateFileNameCount - tupleCount));
                        }
                        bufferStartCount = tupleCount;
                    }
                    
                    while (!buffer.isFull()) {
                        updateFileName();
                        buffer.processTuple(input.readString(), input.readBoolean(), input.readInt(), input.readInt());
                        tupleCount++;
                    }
                } catch(EOFException e) {}
            }

            public final void updateFileName() throws IOException {
                if (updateFileNameCount > tupleCount)
                    return;
                     
                last.fileName = input.readString();
                updateFileNameCount = tupleCount + input.readInt();
                                      
                buffer.processFileName(last.fileName);
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
                } else if (processor instanceof DocumentSplit.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((DocumentSplit.Processor) processor));
                } else if (processor instanceof julien.galago.tupleflow.Processor) {
                    this.processor = new DuplicateEliminator(new TupleUnshredder((julien.galago.tupleflow.Processor<DocumentSplit>) processor));
                } else {
                    throw new IncompatibleProcessorException(processor.getClass().getName() + " is not supported by " + this.getClass().getName());                                                                       
                }
                return processor;
            }                                
            
            public Class<DocumentSplit> getOutputClass() {
                return DocumentSplit.class;
            }                
        }
        
        public static class DuplicateEliminator implements ShreddedProcessor {
            public ShreddedProcessor processor;
            DocumentSplit last = new DocumentSplit();
            boolean fileNameProcess = true;
                                           
            public DuplicateEliminator() {}
            public DuplicateEliminator(ShreddedProcessor processor) {
                this.processor = processor;
            }
            
            public void setShreddedProcessor(ShreddedProcessor processor) {
                this.processor = processor;
            }

            public void processFileName(String fileName) throws IOException {  
                if (fileNameProcess || Utility.compare(fileName, last.fileName) != 0) {
                    last.fileName = fileName;
                    processor.processFileName(fileName);
                    fileNameProcess = false;
                }
            }  
            
            public void resetFileName() {
                 fileNameProcess = true;
            }                                                
                               
            public void processTuple(String fileType, boolean isCompressed, int numDocuments, int startDocument) throws IOException {
                processor.processTuple(fileType, isCompressed, numDocuments, startDocument);
            } 
            
            public void close() throws IOException {
                processor.close();
            }                    
        }
        public static class TupleUnshredder implements ShreddedProcessor {
            DocumentSplit last = new DocumentSplit();
            public julien.galago.tupleflow.Processor<DocumentSplit> processor;                               
            
            public TupleUnshredder(DocumentSplit.Processor processor) {
                this.processor = processor;
            }         
            
            public TupleUnshredder(julien.galago.tupleflow.Processor<DocumentSplit> processor) {
                this.processor = processor;
            }
            
            public DocumentSplit clone(DocumentSplit object) {
                DocumentSplit result = new DocumentSplit();
                if (object == null) return result;
                result.fileName = object.fileName; 
                result.fileType = object.fileType; 
                result.isCompressed = object.isCompressed; 
                result.numDocuments = object.numDocuments; 
                result.startDocument = object.startDocument; 
                return result;
            }                 
            
            public void processFileName(String fileName) throws IOException {
                last.fileName = fileName;
            }   
                
            
            public void processTuple(String fileType, boolean isCompressed, int numDocuments, int startDocument) throws IOException {
                last.fileType = fileType;
                last.isCompressed = isCompressed;
                last.numDocuments = numDocuments;
                last.startDocument = startDocument;
                processor.process(clone(last));
            }               
            
            public void close() throws IOException {
                processor.close();
            }
        }     
        public static class TupleShredder implements Processor {
            DocumentSplit last = null;
            public ShreddedProcessor processor;
            
            public TupleShredder(ShreddedProcessor processor) {
                this.processor = processor;
            }                              
            
            public DocumentSplit clone(DocumentSplit object) {
                DocumentSplit result = new DocumentSplit();
                if (object == null) return result;
                result.fileName = object.fileName; 
                result.fileType = object.fileType; 
                result.isCompressed = object.isCompressed; 
                result.numDocuments = object.numDocuments; 
                result.startDocument = object.startDocument; 
                return result;
            }                 
            
            public void process(DocumentSplit object) throws IOException {                                                                                                                                                   
                boolean processAll = false;
                if(last == null || Utility.compare(last.fileName, object.fileName) != 0 || processAll) { processor.processFileName(object.fileName); processAll = true; }
                processor.processTuple(object.fileType, object.isCompressed, object.numDocuments, object.startDocument);                                         
                last = object;
            }
                          
            public Class<DocumentSplit> getInputClass() {
                return DocumentSplit.class;
            }
            
            public void close() throws IOException {
                processor.close();
            }                     
        }
    } 
}    