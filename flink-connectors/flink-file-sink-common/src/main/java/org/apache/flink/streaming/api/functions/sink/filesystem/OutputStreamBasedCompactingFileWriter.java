package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;
import java.io.OutputStream;

public interface OutputStreamBasedCompactingFileWriter extends CompactingFileWriter {
    OutputStream asOutputStream() throws IOException;
}
