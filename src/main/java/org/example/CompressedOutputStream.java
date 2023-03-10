package org.example;

import net.jpountz.lz4.LZ4Compressor;
import net.jpountz.lz4.LZ4Factory;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public class CompressedOutputStream extends FilterOutputStream {

    protected byte[] buf;
    protected int count;
    protected LZ4Factory factory;
    LZ4Compressor compressor;

    public CompressedOutputStream(OutputStream out) {
        this(out, 16*1024*1024);
    }

    public CompressedOutputStream(OutputStream out, int size) {
        super(out);
        if (size <= 0) {
            throw new IllegalArgumentException("Buffer size <= 0");
        }
        buf = new byte[size];
        factory = LZ4Factory.fastestInstance();
        compressor  = factory.highCompressor();
    }

    private void writeCompressedChunk() throws IOException {

        int maxCompressedLength = compressor.maxCompressedLength(count);
        byte[] compressed = new byte[maxCompressedLength];
        int compressedLength = compressor.compress(buf, 0, count, compressed, 0, maxCompressedLength);

        out.write(BytesConverter.IntToByteArray(compressedLength));
        out.write(compressed, 0, compressedLength);
    }

    /** Flush the internal buffer */
    private void flushBuffer() throws IOException {
        if (count > 0) {
            writeCompressedChunk();
            count = 0;
        }
    }

    /**
     * Writes the specified byte to this buffered output stream.
     *
     * @param      b   the byte to be written.
     * @exception  IOException  if an I/O error occurs.
     */
    public synchronized void write(int b) throws IOException {
        if (count >= buf.length) {
            flushBuffer();
        }
        buf[count++] = (byte)b;
    }

    /**
     * Writes <code>len</code> bytes from the specified byte array
     * starting at offset <code>off</code> to this buffered output stream.
     *
     * <p> Ordinarily this method stores bytes from the given array into this
     * stream's buffer, flushing the buffer to the underlying output stream as
     * needed.  If the requested length is at least as large as this stream's
     * buffer, however, then this method will flush the buffer and write the
     * bytes directly to the underlying output stream.  Thus, redundant
     * <code>BufferedOutputStream</code>s will not copy data unnecessarily.
     *
     * @param      b     the data.
     * @param      off   the start offset in the data.
     * @param      len   the number of bytes to write.
     * @exception  IOException  if an I/O error occurs.
     */
    public synchronized void write(byte[] b, int off, int len) throws IOException {
        if (len >= buf.length) {
            throw new IllegalArgumentException("Too long array");
        }
        if (len > buf.length - count) {
            flushBuffer();
        }
        System.arraycopy(b, off, buf, count, len);
        count += len;
    }

    /**
     * Flushes this buffered output stream. This forces any buffered
     * output bytes to be written out to the underlying output stream.
     *
     * @exception  IOException  if an I/O error occurs.
     * @see        java.io.FilterOutputStream#out
     */
    public synchronized void flush() throws IOException {
        flushBuffer();
        out.flush();
    }
}
