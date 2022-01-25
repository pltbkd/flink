package org.apache.flink.connector.file.sink.compactor;

import java.io.Serializable;

public class FileCompactStrategy implements Serializable {

    private final long sizeThreshold;
    // TODO not implemented
    private final long maxIntervalMs;
    // TODO not implemented
    private final boolean allowCrossCheckpoint;

    private final int compactThread;

    private FileCompactStrategy(
            long sizeThreshold,
            long maxIntervalMs,
            boolean allowCrossCheckpoint,
            int compactThread) {
        this.sizeThreshold = sizeThreshold;
        this.maxIntervalMs = maxIntervalMs;
        this.allowCrossCheckpoint = allowCrossCheckpoint;
        this.compactThread = compactThread;
    }

    public long getSizeThreshold() {
        return sizeThreshold;
    }

    public long getMaxIntervalMs() {
        return maxIntervalMs;
    }

    public boolean isAllowCrossCheckpoint() {
        return allowCrossCheckpoint;
    }

    public int getCompactThread() {
        return compactThread;
    }

    public static class Builder {
        private long sizeThreshold = -1;
        private long maxIntervalMs = -1;
        private boolean allowCrossCheckpoint = true;
        private int compactThread = 4;

        public static FileCompactStrategy.Builder newBuilder() {
            return new FileCompactStrategy.Builder();
        }

        public FileCompactStrategy.Builder withSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        public FileCompactStrategy.Builder withMaxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        public FileCompactStrategy.Builder allowCrossCheckpoint(boolean allowCrossCheckpoint) {
            this.allowCrossCheckpoint = allowCrossCheckpoint;
            return this;
        }

        public FileCompactStrategy.Builder compactThread(int compactThread) {
            this.compactThread = compactThread;
            return this;
        }

        public FileCompactStrategy build() {
            return new FileCompactStrategy(
                    sizeThreshold, maxIntervalMs, allowCrossCheckpoint, compactThread);
        }
    }
}
