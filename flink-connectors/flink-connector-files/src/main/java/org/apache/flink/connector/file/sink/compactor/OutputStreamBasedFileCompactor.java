package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedCompactingFileWriter;

import java.io.IOException;
import java.io.OutputStream;
import java.util.List;

public abstract class OutputStreamBasedFileCompactor implements FileCompactor {
    @Override
    public final CompactingFileWriter.Type getWriterType() {
        return CompactingFileWriter.Type.OUTPUT_STREAM;
    }

    @Override
    public void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception {
        OutputStream outputStream =
                ((OutputStreamBasedCompactingFileWriter) writer).asOutputStream();
        doCompact(inputFiles, new CloseShieldOutputStream(outputStream));
    }

    protected abstract void doCompact(List<Path> inputFiles, OutputStream outputStream)
            throws Exception;

    private static class CloseShieldOutputStream extends OutputStream {
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
            // we do not actually close the internal stream here to prevent that the finishing
            // of the Avro Writer closes the target output stream.
        }
    }
}
