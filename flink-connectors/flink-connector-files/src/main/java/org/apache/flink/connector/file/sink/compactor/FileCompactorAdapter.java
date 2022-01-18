package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FSOutputStreamBasedCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FilePathBasedCompactor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class FileCompactorAdapter
        implements Compactor<FileSinkCommittable, FileCompactRequest> {

    public static FileCompactorAdapter forCompactor(FileCompactor<?> fileCompactor) {
        if (fileCompactor instanceof FilePathBasedCompactor) {
            return new FilePathBasedCompactorAdapter((FilePathBasedCompactor) fileCompactor);
        }
        if (fileCompactor instanceof FSOutputStreamBasedCompactor) {
            return new FSOutputStreamBasedCompactorAdapter(
                    (FSOutputStreamBasedCompactor) fileCompactor);
        }
        throw new RuntimeException(
                "Unsupported file compactor:" + fileCompactor.getClass().getName());
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

    protected abstract PendingFileRecoverable doCompact(
            FileCompactRequest request, List<Path> compactingFiles) throws IOException;

    private static class FilePathBasedCompactorAdapter extends FileCompactorAdapter {
        private final FilePathBasedCompactor fileCompactor;

        public FilePathBasedCompactorAdapter(FilePathBasedCompactor fileCompactor) {
            this.fileCompactor = fileCompactor;
        }

        @Override
        protected PendingFileRecoverable doCompact(
                FileCompactRequest request, List<Path> compactingFiles) {
            // TODO HadoopPathBasedPendingFile is implemented inside flink-hadoop-bulk, and is not
            // accessible
            Path targetPath = getPendingPath(request);
            fileCompactor.compact(compactingFiles, targetPath);
            // TODO
            return new HadoopPathBasedPendingFile(targetPath, toCommittedPath(targetPath));
        }
    }

    private static class FSOutputStreamBasedCompactorAdapter extends FileCompactorAdapter {
        private final FSOutputStreamBasedCompactor fileCompactor;

        private FSOutputStreamBasedCompactorAdapter(FSOutputStreamBasedCompactor fileCompactor) {
            this.fileCompactor = fileCompactor;
        }

        @Override
        protected PendingFileRecoverable doCompact(
                FileCompactRequest request, List<Path> compactingFiles) throws IOException {
            // TODO
            RecoverableFsDataOutputStream os = null;
            fileCompactor.compact(compactingFiles, os);
            return new OutputStreamBasedPendingFileRecoverable(
                    os.closeForCommit().getRecoverable());
        }
    }
}
