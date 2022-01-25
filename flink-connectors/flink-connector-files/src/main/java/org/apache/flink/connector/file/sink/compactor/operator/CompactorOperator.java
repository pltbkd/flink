/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.file.sink.compactor.operator;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.compactor.FileCompactor;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.CompactingFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.TreeMap;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

public class CompactorOperator
        extends AbstractStreamOperator<CommittableMessage<FileSinkCommittable>>
        implements OneInputStreamOperator<
                        FileCompactRequest, CommittableMessage<FileSinkCommittable>>,
                BoundedOneInput,
                CheckpointListener {

    private static final String COMPACTED_PREFIX = "compacted-";
    private static final long SUBMITTED_ID = -1L;

    private static final ListStateDescriptor<byte[]> REMAINING_REQUESTS_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "remaining_requests_raw_state", BytePrimitiveArraySerializer.INSTANCE);

    private final int compactThreads;
    private final SimpleVersionedSerializer<FileSinkCommittable> committableSerializer;

    private transient ExecutorService compactService;

    // TODO not support unaligned checkpoint at present
    private final TreeMap<Long, List<FileCompactRequest>> receivedRequests = new TreeMap<>();
    private final List<Tuple2<FileCompactRequest, CompletableFuture<Iterable<FileSinkCommittable>>>>
            compactingRequests = new LinkedList<>();

    private long lastCheckpointId = 0;

    private ListState<Map<Long, List<FileCompactRequest>>> remainingRequestsState;

    private final FileCompactor fileCompactor;
    private final BucketWriter<?, String> bucketWriter;

    public CompactorOperator(
            int compactThreads,
            SimpleVersionedSerializer<FileSinkCommittable> committableSerializer,
            FileCompactor fileCompactor,
            BucketWriter<?, String> bucketWriter) {
        this.compactThreads = compactThreads;
        this.committableSerializer = committableSerializer;
        this.fileCompactor = fileCompactor;
        this.bucketWriter = bucketWriter;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.compactService =
                Executors.newFixedThreadPool(
                        Math.max(1, Math.min(compactThreads, Hardware.getNumberCPUCores())),
                        new ExecutorThreadFactory("compact-executor"));
    }

    @Override
    public void processElement(StreamRecord<FileCompactRequest> element) throws Exception {
        List<FileCompactRequest> cpRequests =
                receivedRequests.computeIfAbsent(lastCheckpointId, id -> new ArrayList<>());
        cpRequests.add(element.getValue());
    }

    @Override
    public void endInput() throws Exception {
        submitUntil(Long.MAX_VALUE);
        assert receivedRequests.isEmpty();

        CompletableFuture.allOf(
                        compactingRequests.stream()
                                .map(r -> r.f1)
                                .toArray(CompletableFuture[]::new))
                .join();
        emitCompacted(null);
        assert compactingRequests.isEmpty();
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) throws Exception {
        super.notifyCheckpointComplete(checkpointId);
        submitUntil(checkpointId);
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        emitCompacted(checkpointId);
    }

    private void submitUntil(long checkpointId) {
        NavigableMap<Long, List<FileCompactRequest>> canSubmit =
                receivedRequests.subMap(Long.MIN_VALUE, true, checkpointId, true);
        Iterator<Entry<Long, List<FileCompactRequest>>> iter = canSubmit.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, List<FileCompactRequest>> requestEntry = iter.next();
            requestEntry.getValue().forEach(this::submit);
            iter.remove();
        }
    }

    private void submit(FileCompactRequest req) {
        CompletableFuture<Iterable<FileSinkCommittable>> resultFuture = new CompletableFuture<>();
        compactingRequests.add(new Tuple2<>(req, resultFuture));
        compactService.submit(
                () -> {
                    try {
                        Iterable<FileSinkCommittable> result = compact(req);
                        resultFuture.complete(result);
                    } catch (Exception e) {
                        resultFuture.completeExceptionally(e);
                    }
                });
    }

    private void emitCompacted(@Nullable Long checkpointId) throws Exception {
        List<FileSinkCommittable> compacted = new ArrayList<>();
        Iterator<Tuple2<FileCompactRequest, CompletableFuture<Iterable<FileSinkCommittable>>>>
                iter = compactingRequests.iterator();
        while (iter.hasNext()) {
            Tuple2<FileCompactRequest, CompletableFuture<Iterable<FileSinkCommittable>>>
                    compacting = iter.next();
            CompletableFuture<Iterable<FileSinkCommittable>> future = compacting.f1;
            if (future.isDone()) {
                iter.remove();
                // Exception is thrown if it's completed exceptionally
                for (FileSinkCommittable c : future.get()) {
                    compacted.add(c);
                }
            }
        }

        // A summary must be sent before all results during this checkpoint
        CommittableSummary<FileSinkCommittable> summary =
                new CommittableSummary<>(
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        checkpointId,
                        compacted.size(),
                        compacted.size());
        output.collect(new StreamRecord<>(summary));
        for (FileSinkCommittable c : compacted) {
            CommittableWithLineage<FileSinkCommittable> comm =
                    new CommittableWithLineage<>(
                            c, checkpointId, getRuntimeContext().getIndexOfThisSubtask());
            output.collect(new StreamRecord<>(comm));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        lastCheckpointId = context.getCheckpointId();
        Map<Long, List<FileCompactRequest>> requests = new HashMap<>(receivedRequests);
        requests.computeIfAbsent(SUBMITTED_ID, id -> new ArrayList<>())
                .addAll(compactingRequests.stream().map(r -> r.f0).collect(Collectors.toList()));
        remainingRequestsState.update(Collections.singletonList(requests));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        remainingRequestsState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_REQUESTS_RAW_STATES_DESC),
                        new RemainingRequestsSerializer(
                                new FileCompactRequestSerializer(committableSerializer)));

        Iterable<Map<Long, List<FileCompactRequest>>> stateRemaining = remainingRequestsState.get();
        if (stateRemaining != null) {
            for (Map<Long, List<FileCompactRequest>> requests : stateRemaining) {
                // elements can be more than one when redistributed after parallelism changing
                for (Map.Entry<Long, List<FileCompactRequest>> e : requests.entrySet()) {
                    List<FileCompactRequest> list =
                            receivedRequests.computeIfAbsent(e.getKey(), id -> new ArrayList<>());
                    list.addAll(e.getValue());
                }
            }
        }
        // submit all requests that is already submitted before this checkpoint
        submitUntil(SUBMITTED_ID);
    }

    public Iterable<FileSinkCommittable> compact(FileCompactRequest request) throws Exception {
        List<FileSinkCommittable> results = new ArrayList<>();
        List<FileSinkCommittable> compactingCommittables = new ArrayList<>();

        for (FileSinkCommittable committable : request.getCommittableList()) {
            if (committable.hasInProgressFileToCleanup()) {
                results.add(
                        new FileSinkCommittable(
                                request.getBucketId(), committable.getInProgressFileToCleanup()));
            }

            if (committable.hasPendingFile()) {
                compactingCommittables.add(committable);
            }
        }

        List<Path> compactingFiles = getCompactingPath(request, compactingCommittables, results);

        if (compactingFiles.isEmpty()) {
            return results;
        }

        PendingFileRecoverable compactedPendingFile = doCompact(request, compactingFiles);
        FileSinkCommittable compacted =
                new FileSinkCommittable(request.getBucketId(), compactedPendingFile);
        results.add(0, compacted);
        for (Path f : compactingFiles) {
            // cleanup compacted files
            results.add(new FileSinkCommittable(request.getBucketId(), f));
        }

        // TODO add compacted pending files to remove
        return results;
    }

    // results: side output pass through committables
    private List<Path> getCompactingPath(
            FileCompactRequest request,
            List<FileSinkCommittable> compactingCommittables,
            List<FileSinkCommittable> results)
            throws IOException {
        List<Path> compactingFiles = new ArrayList<>();

        for (FileSinkCommittable committable : compactingCommittables) {
            // TODO directly check if the committed path is a visible one?
            if (committable.getPendingFile().getPath() == null
                    || !committable.getPendingFile().getPath().getName().startsWith(".")) {
                // the file may be written with writer of elder version, or
                // the file will be visible once committed, so it can not be compacted.
                // pass through, add to results, do not add to compacting files
                results.add(committable);
            } else {
                // commit the pending file and compact the committed file
                bucketWriter.recoverPendingFile(committable.getPendingFile()).commitAfterRecovery();
                compactingFiles.add(committable.getPendingFile().getPath());
            }
        }
        return compactingFiles;
    }

    private PendingFileRecoverable doCompact(FileCompactRequest request, List<Path> compactingFiles)
            throws Exception {
        Path targetPath = assembleCompactedFilePath(compactingFiles.get(0));
        CompactingFileWriter compactingFileWriter =
                bucketWriter.openNewCompactingFile(
                        fileCompactor.getWriterType(),
                        request.getBucketId(),
                        targetPath,
                        System.currentTimeMillis());
        fileCompactor.compact(compactingFiles, compactingFileWriter);
        return compactingFileWriter.closeForCommit();
    }

    private static Path assembleCompactedFilePath(Path uncompactedPath) {
        // TODO verify
        return new Path(uncompactedPath.getParent(), COMPACTED_PREFIX + uncompactedPath.getName());
    }

    private static class RemainingRequestsSerializer
            implements SimpleVersionedSerializer<Map<Long, List<FileCompactRequest>>> {

        private static final int MAGIC_NUMBER = 0xa946be83;

        private final FileCompactRequestSerializer requestSerializer;

        private RemainingRequestsSerializer(FileCompactRequestSerializer requestSerializer) {
            this.requestSerializer = requestSerializer;
        }

        @Override
        public int getVersion() {
            return 1;
        }

        @Override
        public byte[] serialize(Map<Long, List<FileCompactRequest>> remainingRequests)
                throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeInt(MAGIC_NUMBER);
            serializeV1(remainingRequests, out);
            return out.getCopyOfBuffer();
        }

        @Override
        public Map<Long, List<FileCompactRequest>> deserialize(int version, byte[] serialized)
                throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);

            switch (version) {
                case 1:
                    validateMagicNumber(in);
                    return deserializeV1(in);
                default:
                    throw new IOException("Unrecognized version or corrupt state: " + version);
            }
        }

        private void serializeV1(
                Map<Long, List<FileCompactRequest>> request, DataOutputSerializer out)
                throws IOException {
            out.writeInt(request.size());
            for (Map.Entry<Long, List<FileCompactRequest>> e : request.entrySet()) {
                out.writeLong(e.getKey());
                SimpleVersionedSerialization.writeVersionAndSerializeList(
                        requestSerializer, e.getValue(), out);
            }
        }

        private Map<Long, List<FileCompactRequest>> deserializeV1(DataInputDeserializer in)
                throws IOException {
            int size = in.readInt();
            ;
            Map<Long, List<FileCompactRequest>> requestMap = new HashMap<>(size);
            for (int i = 0; i < size; i++) {
                long cpId = in.readLong();
                List<FileCompactRequest> requests =
                        SimpleVersionedSerialization.readVersionAndDeserializeList(
                                requestSerializer, in);
                requestMap.put(cpId, requests);
            }
            return requestMap;
        }

        private static void validateMagicNumber(DataInputView in) throws IOException {
            int magicNumber = in.readInt();
            if (magicNumber != MAGIC_NUMBER) {
                throw new IOException(
                        String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
            }
        }
    }
}
