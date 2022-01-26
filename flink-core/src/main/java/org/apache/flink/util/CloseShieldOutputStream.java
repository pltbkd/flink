package org.apache.flink.util;

import java.io.IOException;
import java.io.OutputStream;

/** A proxy output stream that prevents the underlying output stream from being closed. */
public class CloseShieldOutputStream extends OutputStream {
    private final OutputStream out;

    public CloseShieldOutputStream(OutputStream out) {
        this.out = out;
    }

    @Override
    public void write(int b) throws IOException {
        out.write(b);
    }

    @Override
    public void write(byte[] buffer) throws IOException {
        out.write(buffer);
    }

    @Override
    public void write(byte[] buffer, int off, int len) throws IOException {
        out.write(buffer, off, len);
    }

    @Override
    public void flush() throws IOException {
        out.flush();
    }

    @Override
    public void close() throws IOException {
        // Do not actually close the internal stream.
    }
}
