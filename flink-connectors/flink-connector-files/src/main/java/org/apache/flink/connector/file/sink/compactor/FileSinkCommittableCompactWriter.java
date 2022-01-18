package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.ReadingElementsCompactor.FileCompactWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;

public class FileSinkCommittableCompactWriter<InputT> implements FileCompactWriter<InputT> {
    private final InProgressFileWriter<InputT, String> fileWriter;

    public FileSinkCommittableCompactWriter(InProgressFileWriter<InputT, String> fileWriter) {
        this.fileWriter = fileWriter;
    }

    @Override
    public void write(InputT record) throws IOException {
        fileWriter.write(record, System.currentTimeMillis());
    }

    @Override
    public PendingFileRecoverable closeForCommit() throws IOException {
        return fileWriter.closeForCommit();
    }

    public static class Factory<InputT> implements FileCompactWriter.Factory<InputT> {
        public static final String COMPACTED_PREFIX = "compacted-";

        private final SerializableSupplierWithException<BucketWriter<InputT, String>, IOException>
                bucketWriterSupplier;

        private transient BucketWriter<InputT, String> bucketWriter;

        public Factory(
                SerializableSupplierWithException<BucketWriter<InputT, String>, IOException>
                        bucketWriterSupplier) {
            this.bucketWriterSupplier = bucketWriterSupplier;
        }

        @Override
        public FileCompactWriter<InputT> create(FileCompactRequest request) throws IOException {
            if (bucketWriter == null) {
                synchronized (bucketWriterSupplier) {
                    if (bucketWriter == null) {
                        bucketWriter = bucketWriterSupplier.get();
                    }
                }
            }

            // TODO verify
            assert request.getCommittable() != null;
            for (FileSinkCommittable committable : request.getCommittable()) {
                if (committable.hasPendingFile()) {
                    Path path =
                            createCompactedFile(
                                    FileCompactorUtil.getPath(committable.getPendingFile()));
                    InProgressFileWriter<InputT, String> fileWriter =
                            bucketWriter.openNewInProgressFile(
                                    request.getBucketId(), path, System.currentTimeMillis());
                    return new FileSinkCommittableCompactWriter<>(fileWriter);
                }
            }
            // TODO verify
            throw new IllegalArgumentException("request contains no files to compact");
        }

        private static Path createCompactedFile(Path uncompactedPath) {
            // TODO verify
            return new Path(
                    uncompactedPath.getParent(), COMPACTED_PREFIX + uncompactedPath.getName());
        }
    }
}
