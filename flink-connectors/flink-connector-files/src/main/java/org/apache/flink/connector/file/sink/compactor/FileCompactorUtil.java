package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.core.fs.Path;

public class FileCompactorUtil {
    public static final String COMPACTED_PREFIX = "compacted-";

    public static Path assembleCompactedFilePath(Path uncompactedPath) {
        // TODO verify
        return new Path(uncompactedPath.getParent(), COMPACTED_PREFIX + uncompactedPath.getName());
    }

}
