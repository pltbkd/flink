package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class FileSinkCompactor<InputT>
        implements Compactor<FileSinkCommittable, FileCompactRequest> {
    private final FileCompactor fileCompactor;
    private final BucketWriter<InputT, String> bucketWriter;
    private boolean commitBeforeCompact = true;

    public FileSinkCompactor(
            FileCompactor fileCompactor, BucketWriter<InputT, String> bucketWriter) {
        this.fileCompactor = fileCompactor;
        this.bucketWriter = bucketWriter;
    }

    @Override
    public Iterable<FileSinkCommittable> compact(FileCompactRequest request) throws Exception {
        List<FileSinkCommittable> results = new ArrayList<>();
        List<FileSinkCommittable> compactingCommittables = new ArrayList<>();

        for (FileSinkCommittable committable : request.getCommittable()) {
            if (committable.hasInProgressFileToCleanup()) {
                results.add(
                        new FileSinkCommittable(
                                request.getBucketId(), committable.getInProgressFileToCleanup()));
            }

            if (committable.hasPendingFile()) {
                compactingCommittables.add(committable);
            }
        }

        List<Path> compactingFiles = getCompactingPath(request, compactingCommittables, results);

        if (compactingFiles.isEmpty()) {
            return results;
        }

        PendingFileRecoverable compactedPendingFile = doCompact(request, compactingFiles);
        FileSinkCommittable compacted =
                new FileSinkCommittable(request.getBucketId(), compactedPendingFile);
        results.add(0, compacted);

        // TODO add compacted pending files to remove
        return results;
    }

    // results: side output pass through committables
    private List<Path> getCompactingPath(
            FileCompactRequest request,
            List<FileSinkCommittable> compactingCommittables,
            List<FileSinkCommittable> results)
            throws IOException {
        if (!commitBeforeCompact) {
            // return in progress path of these committables
            return compactingCommittables.stream()
                    .map(c -> FileCompactorUtil.getInProgressPath(c.getPendingFile()))
                    .collect(Collectors.toList());
        }

        List<Path> compactingFiles = new ArrayList<>();

        for (FileSinkCommittable committable : compactingCommittables) {
            // TODO directly check if the committed path is a visible one?
            if (!FileCompactorUtil.getCommittedPath(committable.getPendingFile())
                    .getName()
                    .startsWith(".")) {
                // the file will be visible once committed, so it can not be compacted
                // pass through, add to results, do not add to compacting files
                results.add(committable);
            } else {
                // commit the pending file and compact with the committed path
                bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
                compactingFiles.add(
                        FileCompactorUtil.getCommittedPath(committable.getPendingFile()));
            }
        }
        return compactingFiles;
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
