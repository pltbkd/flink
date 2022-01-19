package org.apache.flink.connector.file.sink.compactor;

import java.io.Serializable;

public class CompactStrategy implements Serializable {

    private final long sizeThreshold;
    // TODO not implemented
    private final long maxIntervalMs;
    // TODO not implemented
    private final boolean allowCrossCheckpoint;

    // TODO
    private boolean commitBeforeCompact = false;

    public CompactStrategy(long sizeThreshold, long maxIntervalMs, boolean allowCrossCheckpoint) {
        this.sizeThreshold = sizeThreshold;
        this.maxIntervalMs = maxIntervalMs;
        this.allowCrossCheckpoint = allowCrossCheckpoint;
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

    public boolean isCommitBeforeCompact() {
        return commitBeforeCompact;
    }

    public static class Builder {
        private long sizeThreshold = -1;
        private long maxIntervalMs = -1;
        private boolean allowCrossCheckpoint = true;

        public static CompactStrategy.Builder newBuilder() {
            return new CompactStrategy.Builder();
        }

        public CompactStrategy.Builder withSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        public CompactStrategy.Builder withMaxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        public CompactStrategy.Builder allowCrossCheckpoint(boolean allow) {
            this.allowCrossCheckpoint = allow;
            return this;
        }

        public CompactStrategy build() {
            return new CompactStrategy(sizeThreshold, maxIntervalMs, allowCrossCheckpoint);
        }
    }
}
