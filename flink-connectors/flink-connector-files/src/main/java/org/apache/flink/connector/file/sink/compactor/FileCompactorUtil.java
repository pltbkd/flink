package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.Path;
import org.apache.flink.core.fs.RecoverableWriter;
import org.apache.flink.core.fs.local.LocalRecoverable;
import org.apache.flink.runtime.fs.hdfs.HadoopFsRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable;

public class FileCompactorUtil {

    public static Path getPath(PendingFileRecoverable pendingFileRecoverable) {
        // TODO not supported for bad dependency
        // if(pendingFileRecoverable instanceof HadoopPathBasedPendingFileRecoverable){
        //     return pendingFileRecoverable.getTempFilePath();
        // }

        // OutputStreamBasedInProgressFileRecoverable is covered in this case
        if (pendingFileRecoverable
                instanceof
                OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable) {
            RecoverableWriter.CommitRecoverable commitRecoverable =
                    ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable)
                            .getCommitRecoverable();

            if (commitRecoverable instanceof LocalRecoverable) {
                return new Path(((LocalRecoverable) commitRecoverable).tempFile().getPath());
            } else if (commitRecoverable instanceof HadoopFsRecoverable) {
                // TODO Compile error? : Cannot access org.apache.hadoop.fs.Path
                return new Path(((HadoopFsRecoverable) commitRecoverable).tempFile().toUri());
            }
            // TODO not supported for bad dependency
            // else if (commitRecoverable instanceof S3Recoverable) {}
        }

        throw new UnsupportedOperationException();
    }

    public static long getSize(PendingFileRecoverable pendingFileRecoverable) {
        // TODO not supported for bad dependency
        // if(pendingFileRecoverable instanceof HadoopPathBasedPendingFileRecoverable){
        //     return pendingFileRecoverable.getTempFilePath();
        // }

        // OutputStreamBasedInProgressFileRecoverable is covered in this case
        if (pendingFileRecoverable
                instanceof
                OutputStreamBasedPartFileWriter.OutputStreamBasedPendingFileRecoverable) {
            RecoverableWriter.CommitRecoverable commitRecoverable =
                    ((OutputStreamBasedPendingFileRecoverable) pendingFileRecoverable)
                            .getCommitRecoverable();

            if (commitRecoverable instanceof LocalRecoverable) {
                return ((LocalRecoverable) commitRecoverable).offset();
            } else if (commitRecoverable instanceof HadoopFsRecoverable) {
                // TODO Compile error? : Cannot access org.apache.hadoop.fs.Path
                return ((HadoopFsRecoverable) commitRecoverable).offset();
            }
            // TODO not supported for bad dependency
            // else if (commitRecoverable instanceof S3Recoverable) {}
        }

        throw new UnsupportedOperationException();
    }
}
