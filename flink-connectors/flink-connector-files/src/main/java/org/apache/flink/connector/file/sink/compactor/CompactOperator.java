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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.state.CheckpointListener;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.typeutils.ListTypeInfo;
import org.apache.flink.api.java.typeutils.MapTypeInfo;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.runtime.util.Hardware;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.concurrent.ExecutorThreadFactory;

import javax.annotation.Nullable;
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

public class CompactOperator<CommT, CompT> extends AbstractStreamOperator<CommittableMessage<CommT>>
        implements OneInputStreamOperator<CompT, CommittableMessage<CommT>>,
                BoundedOneInput,
                CheckpointListener {

    private static final long SUBMITTED_ID = -1L;

    private final TypeInformation<CompT> compactRequestTypeInfo;

    private final Compactor<CommT, CompT> compactor;
    private final ExecutorService compactService;

    // TODO not support unaligned checkpoint at present
    private final TreeMap<Long, List<CompT>> receivedRequests = new TreeMap<>();
    private final List<Tuple2<CompT, CompletableFuture<Iterable<CommT>>>> compactingRequests =
            new LinkedList<>();

    private long lastCheckpointId = 0;
    private ListState<Map<Long, List<CompT>>> remainingRequests;

    public CompactOperator(
            Compactor<CommT, CompT> compactor,
            int compactThreads,
            TypeInformation<CompT> compactRequestTypeInfo) {
        this.compactor = compactor;
        this.compactRequestTypeInfo = compactRequestTypeInfo;

        this.compactService =
                Executors.newFixedThreadPool(
                        Math.max(1, Math.min(compactThreads, Hardware.getNumberCPUCores())),
                        new ExecutorThreadFactory("compact-executor"));
    }

    @Override
    public void processElement(StreamRecord<CompT> element) throws Exception {
        List<CompT> cpRequests =
                receivedRequests.computeIfAbsent(lastCheckpointId, id -> new ArrayList<>());
        cpRequests.add(element.getValue());
    }

    @Override
    public void endInput() throws Exception {
        submitUntil(Long.MAX_VALUE);
        assert receivedRequests.isEmpty();
        CompletableFuture.allOf(
                        (CompletableFuture<?>)
                                compactingRequests.stream()
                                        .map(r -> r.f1)
                                        .collect(Collectors.toList()))
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
        NavigableMap<Long, List<CompT>> canSubmit =
                receivedRequests.subMap(Long.MIN_VALUE, true, checkpointId, true);
        Iterator<Entry<Long, List<CompT>>> iter = canSubmit.entrySet().iterator();
        while (iter.hasNext()) {
            Entry<Long, List<CompT>> requestEntry = iter.next();
            requestEntry.getValue().forEach(this::submit);
            iter.remove();
        }
    }

    private void submit(CompT req) {
        CompletableFuture<Iterable<CommT>> resultFuture = new CompletableFuture<>();
        compactingRequests.add(new Tuple2<>(req, resultFuture));
        compactService.submit(
                () -> {
                    try {
                        Iterable<CommT> result = compactor.compact(req);
                        resultFuture.complete(result);
                    } catch (Exception e) {
                        resultFuture.completeExceptionally(e);
                    }
                });
    }

    private void emitCompacted(@Nullable Long checkpointId) throws Exception {
        List<CommT> compacted = new ArrayList<>();
        Iterator<Tuple2<CompT, CompletableFuture<Iterable<CommT>>>> iter =
                compactingRequests.iterator();
        while (iter.hasNext()) {
            Tuple2<CompT, CompletableFuture<Iterable<CommT>>> compacting = iter.next();
            CompletableFuture<Iterable<CommT>> future = compacting.f1;
            if (future.isDone()) {
                iter.remove();
                // Exception is thrown if it's completed exceptionally
                for (CommT c : future.get()) {
                    compacted.add(c);
                }
            }
        }

        // A summary must be sent after all results during this checkpoint
        for (CommT c : compacted) {
            CommittableWithLineage<CommT> comm =
                    new CommittableWithLineage<>(
                            c, checkpointId, getRuntimeContext().getIndexOfThisSubtask());
            output.collect(new StreamRecord<>(comm));
        }
        CommittableSummary<CommT> summary =
                new CommittableSummary<>(
                        getRuntimeContext().getIndexOfThisSubtask(),
                        getRuntimeContext().getNumberOfParallelSubtasks(),
                        checkpointId,
                        compacted.size(),
                        compacted.size());
        output.collect(new StreamRecord<>(summary));
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        lastCheckpointId = context.getCheckpointId();
        Map<Long, List<CompT>> requests = new HashMap<>(receivedRequests);
        requests.computeIfAbsent(SUBMITTED_ID, id -> new ArrayList<>())
                .addAll(compactingRequests.stream().map(r -> r.f0).collect(Collectors.toList()));
        remainingRequests.update(Collections.singletonList(requests));
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        TypeInformation<Map<Long, List<CompT>>> requestType =
                new MapTypeInfo<>(
                        BasicTypeInfo.LONG_TYPE_INFO, new ListTypeInfo<>(compactRequestTypeInfo));
        ListStateDescriptor<Map<Long, List<CompT>>> requestDescriptor =
                new ListStateDescriptor<>("remaining-compact-request-state", requestType);
        remainingRequests = context.getOperatorStateStore().getListState(requestDescriptor);

        Iterable<Map<Long, List<CompT>>> stateRemaining = remainingRequests.get();
        if (stateRemaining != null) {
            for (Map<Long, List<CompT>> requests : stateRemaining) {
                // elements can be more than one when redistributed after parallelism changing
                for (Map.Entry<Long, List<CompT>> e : requests.entrySet()) {
                    List<CompT> list =
                            receivedRequests.computeIfAbsent(e.getKey(), id -> new ArrayList<>());
                    list.addAll(e.getValue());
                }
            }
        }
        // submit all requests that is already submitted before this checkpoint
        submitUntil(SUBMITTED_ID);
    }
}
