package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;

import java.io.Serializable;
import java.util.List;

public interface FileCompactor extends Serializable {

    CompactingFileWriter.Type getWriterType();

    void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception;
}
