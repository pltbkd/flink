package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.CommittableTraits.InProgressPathAware;
import org.apache.flink.core.fs.CommittableTraits.SizeAware;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;

public class FileCompactorUtil {
    public static final String COMPACTED_PREFIX = "compacted-";

    public static Path createCompactedFile(Path uncompactedPath) {
        // TODO verify
        return new Path(
                uncompactedPath.getParent(), COMPACTED_PREFIX + uncompactedPath.getName());
    }

    public static Path getPath(PendingFileRecoverable pendingFileRecoverable) {
        if (pendingFileRecoverable instanceof InProgressPathAware) {
            return ((InProgressPathAware) pendingFileRecoverable).getInProgressPath();
        }

        // OutputStreamBasedInProgressFileRecoverable is covered in this case
        if (pendingFileRecoverable
                instanceof
                OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable) {
            RecoverableWriter.CommitRecoverable commitRecoverable =
                    ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable)
                            .getCommitRecoverable();

            if (commitRecoverable instanceof InProgressPathAware) {
                return ((InProgressPathAware) commitRecoverable).getInProgressPath();
            }
        }

        throw new UnsupportedOperationException();
    }

    public static long getSize(PendingFileRecoverable pendingFileRecoverable) {
        if (pendingFileRecoverable instanceof SizeAware) {
            return ((SizeAware) pendingFileRecoverable).getSize();
        }

        // OutputStreamBasedInProgressFileRecoverable is covered in this case
        if (pendingFileRecoverable
                instanceof
                OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable) {
            RecoverableWriter.CommitRecoverable commitRecoverable =
                    ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable)
                            .getCommitRecoverable();

            if (commitRecoverable instanceof SizeAware) {
                return ((SizeAware) commitRecoverable).getSize();
            }
        }

        throw new UnsupportedOperationException();
    }
}
