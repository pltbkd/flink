package org.apache.flink.core.fs;

public interface CommittableTraits {

    interface InProgressPathAware {
        Path getInProgressPath();

        Path getCommittedPath();
    }

    interface SizeAware {
        long getSize();
    }
}
