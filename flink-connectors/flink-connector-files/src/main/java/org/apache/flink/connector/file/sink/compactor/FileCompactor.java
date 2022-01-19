package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.FSDataOutputStream;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;

import java.io.IOException;
import java.util.List;

public interface FileCompactor<OUT> {

    void compact(List<Path> inputFiles, OUT output) throws Exception;

    interface FilePathBasedCompactor extends FileCompactor<Path> {}

    interface FSOutputStreamBasedCompactor extends FileCompactor<FSDataOutputStream> {}

    interface InProgressFileBasedCompactor<IN>
            extends FileCompactor<InProgressFileWriter<IN, String>> {}

    interface Factory<OUT> {
        FileCompactor<OUT> create() throws IOException;
    }
}
