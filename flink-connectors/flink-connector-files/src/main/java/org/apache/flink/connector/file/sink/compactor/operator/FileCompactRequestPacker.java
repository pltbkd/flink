package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactStrategy;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.stream.Collectors;

public class FileCompactRequestPacker {
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

    public FileCompactRequestPacker(FileCompactStrategy strategy) {
        this.sizeThreshold = strategy.getSizeThreshold();
        this.maxIntervalMs = strategy.getMaxIntervalMs();
        this.allowCrossCheckpoint = strategy.isAllowCrossCheckpoint();
    }

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

            long fileSize = committable.getPendingFile().getSize();
            long bucketSize = packingSize.getOrDefault(bucketId, 0L);
            if (bucketSize + fileSize > sizeThreshold || fileSize < 0) {
                // force trigger a compact if the file size is negative, which means the pending
                // file is produced by an old writer
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

    public List<CommittableMessage<FileSinkCommittable>> getRemaining() {
        return packing.values().stream().flatMap(Collection::stream).collect(Collectors.toList());
    }

    public FileCompactRequest getPackedRequest() {
        return requests.poll();
    }
}
