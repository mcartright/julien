/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */

package julien.galago.tupleflow;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;

/**
 *
 * @author trevor
 */
public class MemoryDataStream implements DataStream {
    byte[] data;
    int offset;
    int length;
    DataInputStream input;
    
    public MemoryDataStream(byte[] data, int offset, int length) {
        assert data != null;
        assert data.length >= offset + length;
        this.data = data;
        this.offset = offset;
        this.length = length;
        this.input = new DataInputStream(new ByteArrayInputStream(data, offset, length));
    }
    
    @Override
    public MemoryDataStream subStream(long subOffset, long subLength) {
        assert subOffset <= length;
        assert subOffset + subLength <= length;
        return new MemoryDataStream(
                data, (int) (offset + subOffset),
                (int) subLength);
    }

    @Override
    public long getPosition() {
        try {
            return length - input.available();
        } catch (IOException ex) {
            return length;
        }
    }

    @Override
    public boolean isDone() {
        try {
            return input.available() == 0;
        } catch (IOException ex) {
            return true;
        }
    }

    @Override
    public long length() {
        return length;
    }

    @Override
    public void seek(long offset) {
        if (offset >= length)
            return;

        try {
            int needToSkip = (int) (offset - getPosition());
            while (needToSkip > 0) {
                int skipped = (int) input.skip(needToSkip);
                needToSkip -= skipped;
            }
        } catch(IOException e) {
        }
    }

    @Override
    public void readFully(byte[] b) throws IOException {
        input.readFully(b);
    }

    @Override
    public void readFully(byte[] b, int off, int len) throws IOException {
        input.readFully(b, off, len);
    }

    @Override
    public int skipBytes(int n) throws IOException {
        return input.skipBytes(n);
    }

    @Override
    public boolean readBoolean() throws IOException {
        return input.readBoolean();
    }

    @Override
    public byte readByte() throws IOException {
        return input.readByte();
    }

    @Override
    public int readUnsignedByte() throws IOException {
        return input.readUnsignedByte();
    }

    @Override
    public short readShort() throws IOException {
        return input.readShort();
    }

    @Override
    public int readUnsignedShort() throws IOException {
        return input.readUnsignedShort();
    }

    @Override
    public char readChar() throws IOException {
        return input.readChar();
    }

    @Override
    public int readInt() throws IOException {
        return input.readInt();
    }

    @Override
    public long readLong() throws IOException {
        return input.readLong();
    }

    @Override
    public float readFloat() throws IOException {
        return input.readFloat();
    }

    @Override
    public double readDouble() throws IOException {
        return input.readDouble();
    }

    @Override
    public String readUTF() throws IOException {
        return input.readUTF();
    }

  @Override
  @Deprecated
  public String readLine() throws IOException {
    return input.readLine();
  }
}
