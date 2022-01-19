package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;

import java.util.ArrayList;
import java.util.List;

public class FileSinkCompactor<InputT>
        implements Compactor<FileSinkCommittable, FileCompactRequest> {
    private final FileCompactor fileCompactor;
    private final BucketWriter<InputT, String> bucketWriter;

    public FileSinkCompactor(
            FileCompactor fileCompactor, BucketWriter<InputT, String> bucketWriter) {
        this.fileCompactor = fileCompactor;
        this.bucketWriter = bucketWriter;
    }

    @Override
    public Iterable<FileSinkCommittable> compact(FileCompactRequest request) throws Exception {
        List<FileSinkCommittable> results = new ArrayList<>();
        List<Path> compactingFiles = new ArrayList<>();

        for (FileSinkCommittable committable : request.getCommittable()) {
            if (committable.hasInProgressFileToCleanup()) {
                results.add(
                        new FileSinkCommittable(
                                request.getBucketId(), committable.getInProgressFileToCleanup()));
            }

            if (committable.hasPendingFile()) {
                compactingFiles.add(FileCompactorUtil.getPath(committable.getPendingFile()));
            }
        }

        PendingFileRecoverable compactedPendingFile = doCompact(request, compactingFiles);
        FileSinkCommittable compacted =
                new FileSinkCommittable(request.getBucketId(), compactedPendingFile);
        results.add(0, compacted);

        // TODO add compacted pending files to remove
        return results;
    }

    private PendingFileRecoverable doCompact(FileCompactRequest request, List<Path> compactingFiles)
            throws Exception {
        Path targetPath = FileCompactorUtil.createCompactedFile(compactingFiles.get(0));
        CompactingFileWriter compactingFileWriter =
                bucketWriter.openNewCompactingFile(
                        fileCompactor.getWriterType(),
                        request.getBucketId(),
                        targetPath,
                        System.currentTimeMillis());
        fileCompactor.compact(compactingFiles, compactingFileWriter);
        return compactingFileWriter.closeForCommit();
    }
}
