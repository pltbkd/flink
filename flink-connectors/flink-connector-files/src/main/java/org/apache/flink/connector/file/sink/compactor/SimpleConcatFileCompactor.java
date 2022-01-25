package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.FSDataInputStream;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;

import javax.annotation.Nullable;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

public class SimpleConcatFileCompactor extends OutputStreamBasedFileCompactor {

    private final byte[] fileDelimiter;

    public SimpleConcatFileCompactor() {
        this(null);
    }

    public SimpleConcatFileCompactor(@Nullable byte[] fileDelimiter) {
        this.fileDelimiter = fileDelimiter;
    }

    @Override
    protected void doCompact(List<Path> inputFiles, OutputStream outputStream) throws Exception {
        FileSystem fs = inputFiles.get(0).getFileSystem();
        for (Path input : inputFiles) {
            try (FSDataInputStream inputStream = fs.open(input)) {
                copy(inputStream, outputStream);
            }
            if (fileDelimiter != null) {
                outputStream.write(fileDelimiter);
            }
        }
    }

    private void copy(InputStream in, OutputStream out) throws IOException {
        byte[] buf = new byte[32 * 1024];
        int length;
        while ((length = in.read(buf)) > 0) {
            out.write(buf, 0, length);
        }
    }
}
