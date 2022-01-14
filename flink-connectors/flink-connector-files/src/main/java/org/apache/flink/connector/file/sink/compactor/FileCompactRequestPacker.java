package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.connector.file.sink.FileSinkCommittable;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

public class FileCompactRequestPacker
        implements CompactRequestPacker<FileSinkCommittable, FileCompactRequest> {
    public static final long EOI = Long.MAX_VALUE;

    private final long sizeThreshold;
    // TODO not implemented
    private final long maxIntervalMs;
    // TODO not implemented
    private final boolean allowCrossCheckpoint;

    // TODO make this a collector or bin
    private final Map<String, List<CommittableMessage<FileSinkCommittable>>> packing =
            new HashMap<>();
    private final Map<String, Long> packingSize = new HashMap<>();

    private final Queue<FileCompactRequest> requests = new LinkedList<>();

    public FileCompactRequestPacker(
            long sizeThreshold, long maxIntervalMs, boolean allowCrossCheckpoint) {
        this.sizeThreshold = sizeThreshold;
        this.maxIntervalMs = maxIntervalMs;
        this.allowCrossCheckpoint = allowCrossCheckpoint;
    }

    @Override
    public void accept(CommittableMessage<FileSinkCommittable> message) {
        if (message instanceof CommittableWithLineage) {
            FileSinkCommittable committable =
                    ((CommittableWithLineage<FileSinkCommittable>) message).getCommittable();
            String bucketId = committable.getBucketId();
            if (committable.hasInProgressFileToCleanup()) {
                // cleanup request, pass through directly
                packing.computeIfAbsent(bucketId, id -> new ArrayList<>()).add(message);
                return;
            }

            if (!committable.hasPendingFile()) {
                // TODO verify
                throw new IllegalArgumentException();
            }

            long fileSize = FileCompactorUtil.getSize(committable.getPendingFile());
            long bucketSize = packingSize.getOrDefault(bucketId, 0L);
            if (bucketSize + fileSize > sizeThreshold) {
                List<FileSinkCommittable> committableToCompact = new ArrayList<>();
                packing.get(bucketId)
                        .forEach(
                                m -> {
                                    if (m instanceof CommittableWithLineage)
                                        committableToCompact.add(
                                                ((CommittableWithLineage<FileSinkCommittable>) m)
                                                        .getCommittable());
                                });
                requests.add(new FileCompactRequest(bucketId, committableToCompact));
                packing.remove(bucketId);
                packingSize.remove(bucketId);
            } else {
                packing.computeIfAbsent(bucketId, id -> new ArrayList<>()).add(message);
                packingSize.put(bucketId, bucketSize + fileSize);
            }
        } else if (message instanceof CommittableSummary) {
            // may be used when accumulate only inside a cp, not used yet
            // ignored
        }
    }

    @Override
    public void endInput() {
        for (Map.Entry<String, List<CommittableMessage<FileSinkCommittable>>> packingEntry :
                packing.entrySet()) {
            String bucketId = packingEntry.getKey();
            List<CommittableMessage<FileSinkCommittable>> messages = packingEntry.getValue();

            List<FileSinkCommittable> committableToCompact = new ArrayList<>();
            messages.forEach(
                    m -> {
                        if (m instanceof CommittableWithLineage)
                            committableToCompact.add(
                                    ((CommittableWithLineage<FileSinkCommittable>) m)
                                            .getCommittable());
                    });
            requests.add(new FileCompactRequest(bucketId, committableToCompact));
        }
    }

    @Override
    public List<CommittableMessage<FileSinkCommittable>> getRemaining() {
        return packing.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    @Override
    public FileCompactRequest getPackedRequest() {
        return requests.poll();
    }

    public static class Builder {
        private long sizeThreshold = -1;
        private long maxIntervalMs = -1;
        private boolean allowCrossCheckpoint = true;

        public static Builder newBuilder() {
            return new Builder();
        }

        public Builder withSizeThreshold(long sizeThreshold) {
            this.sizeThreshold = sizeThreshold;
            return this;
        }

        public Builder withMaxIntervalMs(long maxIntervalMs) {
            this.maxIntervalMs = maxIntervalMs;
            return this;
        }

        public Builder allowCrossCheckpoint(boolean allow) {
            this.allowCrossCheckpoint = allow;
            return this;
        }

        public FileCompactRequestPacker build() {
            return new FileCompactRequestPacker(sizeThreshold, maxIntervalMs, allowCrossCheckpoint);
        }
    }
}
