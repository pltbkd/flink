/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.connector.file.sink.FileSink;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkArgument;

/** Strategy for compacting the files written in {@link FileSink} before committing. */
public class FileCompactStrategy implements Serializable {

    private static final long serialVersionUID = 1L;

    // Compaction triggering strategies.
    private final long sizeThreshold;
    private final long maxIntervalMs;
    private final boolean crossCheckpoints;

    // Compaction executing strategies.
    private final int compactThread;

    private FileCompactStrategy(
            long sizeThreshold, long maxIntervalMs, boolean crossCheckpoints, int compactThread) {
        this.sizeThreshold = sizeThreshold;
        this.maxIntervalMs = maxIntervalMs;
        this.crossCheckpoints = crossCheckpoints;
        this.compactThread = compactThread;
    }

    public long getSizeThreshold() {
        return sizeThreshold;
    }

    public long getMaxIntervalMs() {
        return maxIntervalMs;
    }

    public boolean isCrossCheckpoints() {
        return crossCheckpoints;
    }

    public int getCompactThread() {
        return compactThread;
    }

    /** Builder for {@link FileCompactStrategy}. */
    public static class Builder {
        private long sizeThreshold = -1;
        private long maxIntervalMs = -1;
        private boolean crossCheckpoints = true;
        private int compactThread = 1;

        public static FileCompactStrategy.Builder newBuilder() {
            return new FileCompactStrategy.Builder();
        }

        /**
         * Optional, compaction will be triggered when the total size of compacting files reaches
         * the threshold. -1 by default, indicating the size is unlimited.
         */
        public FileCompactStrategy.Builder withSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        /**
         * Optional, max interval since the last compaction triggering. -1 by default, indicating
         * the interval is unlimited. The triggering will actually happen at the next checkpoint.
         */
        public FileCompactStrategy.Builder withMaxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        /** Optional, whether to compact files of different checkpoints, true by default. */
        public FileCompactStrategy.Builder setCrossCheckpoints(boolean crossCheckpoints) {
            this.crossCheckpoints = crossCheckpoints;
            return this;
        }

        /** Optional, the count of compacting threads in a compactor operator, 1 by default. */
        public FileCompactStrategy.Builder withCompactThread(int compactThread) {
            checkArgument(compactThread > 0, "Compact thread should be more than 0.");
            this.compactThread = compactThread;
            return this;
        }

        public FileCompactStrategy build() {
            validate();
            return new FileCompactStrategy(
                    sizeThreshold, maxIntervalMs, crossCheckpoints, compactThread);
        }

        private void validate() {
            if (sizeThreshold < 0 && maxIntervalMs < 0) {
                throw new IllegalArgumentException(
                        "At least one of the sizeThreshold and the maxIntervalMs should be configured.");
            }
        }
    }
}
