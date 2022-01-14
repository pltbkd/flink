package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FileCompactWriter;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;

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

        private final BucketWriter<InputT, String> bucketWriter;

        public Factory(BucketWriter<InputT, String> bucketWriter) {
            this.bucketWriter = bucketWriter;
        }

        @Override
        public FileCompactWriter<InputT> create(FileCompactRequest request) throws IOException {
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
