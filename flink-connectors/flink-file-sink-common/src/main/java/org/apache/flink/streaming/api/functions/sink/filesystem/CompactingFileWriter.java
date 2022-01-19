package org.apache.flink.streaming.api.functions.sink.filesystem;

import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;

import java.io.IOException;

public interface CompactingFileWriter {

    PendingFileRecoverable closeForCommit() throws IOException;

    enum Type{
        RECORD_WISE, OUTPUT_STREAM
    }
}
