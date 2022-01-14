package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.table.stream.compact.CompactReader;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;

import java.io.Closeable;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

public class FileCompactor<InputT> implements Compactor<FileSinkCommittable, FileCompactRequest> {
    private final FileCompactReader.Factory<InputT> readerFactory;
    private final FileCompactWriter.Factory<InputT> writerFactory;

    public FileCompactor(
            Configuration config,
            CompactReader.Factory<InputT> elementReaderFactory,
            BucketWriter<InputT, String> bucketWriter) {
        this(
                new FileSinkCommittableReader.Factory<>(config, elementReaderFactory),
                new FileSinkCommittableCompactWriter.Factory<>(bucketWriter));
    }

    public FileCompactor(
            FileCompactReader.Factory<InputT> readerFactory,
            FileCompactWriter.Factory<InputT> writerFactory) {
        this.readerFactory = readerFactory;
        this.writerFactory = writerFactory;
    }

    @Override
    public Iterable<FileSinkCommittable> compact(FileCompactRequest request) throws Exception {
        List<FileSinkCommittable> results = new ArrayList<>();

        FileCompactWriter<InputT> writer = writerFactory.create(request);

        for (FileSinkCommittable committable : request.getCommittable()) {
            if (committable.hasInProgressFileToCleanup()) {
                results.add(new FileSinkCommittable(committable.getInProgressFileToCleanup()));
            }

            if (committable.hasPendingFile()) {
                try (FileCompactReader<InputT> reader =
                        readerFactory.create(request, committable.getPendingFile())) {
                    InputT record;
                    while ((record = reader.read()) != null) {
                        writer.write(record);
                    }
                }

                // TODO add a cleanup request for the compacted pending file
                // results.add(new FileSinkCommittable(committable.getPendingFile()));
            }
        }

        FileSinkCommittable compacted = new FileSinkCommittable(writer.closeForCommit());
        results.add(0, compacted);
        return results;
    }

    /** Reader for compaction. */
    @Internal
    public interface FileCompactReader<InputT> extends Closeable {

        InputT read() throws IOException;

        interface Factory<InputT> extends Serializable {
            FileCompactReader<InputT> create(
                    FileCompactRequest request, PendingFileRecoverable pendingFileRecoverable)
                    throws IOException;
        }
    }

    /** Writer for compaction. */
    @Internal
    public interface FileCompactWriter<InputT> {

        void write(InputT record) throws IOException;

        PendingFileRecoverable closeForCommit() throws IOException;

        interface Factory<InputT> extends Serializable {
            FileCompactWriter<InputT> create(FileCompactRequest request) throws IOException;
        }
    }
}
