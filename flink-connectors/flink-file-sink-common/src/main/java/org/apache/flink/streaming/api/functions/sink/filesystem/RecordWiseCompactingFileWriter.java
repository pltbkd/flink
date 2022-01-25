package org.apache.flink.streaming.api.functions.sink.filesystem;

import java.io.IOException;

public interface RecordWiseCompactingFileWriter<IN> extends CompactingFileWriter {
    void write(IN element) throws IOException;
}
