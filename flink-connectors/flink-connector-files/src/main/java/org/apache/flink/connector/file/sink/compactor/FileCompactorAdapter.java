package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FSOutputStreamBasedCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.FilePathBasedCompactor;
import org.apache.flink.connector.file.sink.compactor.FileCompactor.InProgressFileBasedCompactor;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableFsDataOutputStream;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;
import org.apache.flink.util.function.SerializableSupplierWithException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

public abstract class FileCompactorAdapter
        implements Compactor<FileSinkCommittable, FileCompactRequest> {

    public static FileCompactorAdapter forFilePathBasedCompactor(
            FilePathBasedCompactor fileCompactor) {
        return new FilePathBasedCompactorAdapter(fileCompactor);
    }

    public static FileCompactorAdapter forFSOutputStreamBasedCompactor(
            FSOutputStreamBasedCompactor fileCompactor) {
        return new FSOutputStreamBasedCompactorAdapter(fileCompactor);
    }

    public static <InputT> FileCompactorAdapter forInProgressFileBasedCompactor(
            InProgressFileBasedCompactor<InputT> fileCompactor,
            SerializableSupplierWithException<BucketWriter<InputT, String>, IOException>
                    bucketWriterSupplier) {
        return new InProgressFileBasedCompactorAdapter<>(fileCompactor, bucketWriterSupplier);
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
            FileCompactRequest request, List<Path> compactingFiles) throws Exception;

    private static class FilePathBasedCompactorAdapter extends FileCompactorAdapter {
        private final FilePathBasedCompactor fileCompactor;

        public FilePathBasedCompactorAdapter(FilePathBasedCompactor fileCompactor) {
            this.fileCompactor = fileCompactor;
        }

        @Override
        protected PendingFileRecoverable doCompact(
                FileCompactRequest request, List<Path> compactingFiles) throws Exception {
            Path targetPath = FileCompactorUtil.createCompactedPendingFile(compactingFiles.get(0));
            fileCompactor.compact(compactingFiles, targetPath);
            return createPendingFile(targetPath);
        }

        private PendingFileRecoverable createPendingFile(Path targetPath) {
            //TODO
            return null;
        }
    }

    private static class FSOutputStreamBasedCompactorAdapter extends FileCompactorAdapter {
        private final FSOutputStreamBasedCompactor fileCompactor;

        private FSOutputStreamBasedCompactorAdapter(FSOutputStreamBasedCompactor fileCompactor) {
            this.fileCompactor = fileCompactor;
        }

        @Override
        protected PendingFileRecoverable doCompact(
                FileCompactRequest request, List<Path> compactingFiles) throws Exception {
            Path targetPath = FileCompactorUtil.createCompactedPendingFile(compactingFiles.get(0));
            FileSystem fs = targetPath.getFileSystem();
            RecoverableWriter writer = fs.createRecoverableWriter();

            RecoverableFsDataOutputStream out = writer.open(targetPath);
            fileCompactor.compact(compactingFiles, out);
            return new OutputStreamBasedPendingFileRecoverable(
                    out.closeForCommit().getRecoverable());
        }
    }

    private static class InProgressFileBasedCompactorAdapter<InputT> extends FileCompactorAdapter {
        private final InProgressFileBasedCompactor<InputT> fileCompactor;
        private final SerializableSupplierWithException<BucketWriter<InputT, String>, IOException>
                bucketWriterSupplier;

        private BucketWriter<InputT, String> bucketWriter;

        private InProgressFileBasedCompactorAdapter(
                InProgressFileBasedCompactor<InputT> fileCompactor,
                SerializableSupplierWithException<BucketWriter<InputT, String>, IOException>
                        bucketWriterSupplier) {
            this.fileCompactor = fileCompactor;
            this.bucketWriterSupplier = bucketWriterSupplier;
        }

        @Override
        protected PendingFileRecoverable doCompact(
                FileCompactRequest request, List<Path> compactingFiles) throws Exception {
            // TODO
            if (bucketWriter == null) {
                synchronized (bucketWriterSupplier) {
                    if (bucketWriter == null) {
                        bucketWriter = bucketWriterSupplier.get();
                    }
                }
            }
            Path targetPath = FileCompactorUtil.createCompactedPendingFile(compactingFiles.get(0));
            InProgressFileWriter<InputT, String> fileWriter =
                    bucketWriter.openNewInProgressFile(
                            request.getBucketId(), targetPath, System.currentTimeMillis());
            fileCompactor.compact(compactingFiles, fileWriter);
            return fileWriter.closeForCommit();
        }
    }
}
