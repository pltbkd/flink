package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter.Type;
import org.apache.flink.streaming.api.functions.sink.filesystem.RecordWiseCompactingFileWriter;

import java.io.IOException;
import java.io.Serializable;
import java.util.List;

public class RecordWiseFileCompactor<IN> implements FileCompactor {
    private final Reader.Factory<IN> readerFactory;

    public RecordWiseFileCompactor(Reader.Factory<IN> readerFactory) {
        this.readerFactory = readerFactory;
    }

    @Override
    public final Type getWriterType() {
        return Type.RECORD_WISE;
    }

    @Override
    public void compact(List<Path> inputFiles, CompactingFileWriter writer) throws Exception {
        RecordWiseCompactingFileWriter<IN> recordWriter = (RecordWiseCompactingFileWriter<IN>) writer;
        for (Path input : inputFiles) {
            try (Reader<IN> reader = readerFactory.open(input)) {
                IN elem;
                while ((elem = reader.read()) != null) {
                    recordWriter.write(elem);
                }
            }
        }
    }

    public interface Reader<T> extends AutoCloseable {
        T read() throws IOException;

        interface Factory<T> extends Serializable {
            Reader<T> open(Path path) throws IOException;
        }
    }
}
