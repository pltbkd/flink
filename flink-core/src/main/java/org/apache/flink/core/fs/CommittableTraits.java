package org.apache.flink.core.fs;

public interface CommittableTraits {

    interface InProgressPathAware {
        Path getInProgressPath();
    }

    interface SizeAware {
        long getSize();
    }
}
