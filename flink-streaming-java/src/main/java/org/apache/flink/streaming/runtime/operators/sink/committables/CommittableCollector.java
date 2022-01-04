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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.api.connector.sink2.Committer;
import org.apache.flink.api.connector.sink2.Committer.CommitRequest;
import org.apache.flink.api.connector.sink2.Sink.InitContext;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NavigableMap;
import java.util.OptionalLong;
import java.util.TreeMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

@Internal
public class CommittableCollector<CommT> {
    private static final long EOI = Long.MAX_VALUE;
    private final NavigableMap<Long, Checkpoint<CommT>> checkpointCommittables;
    private final int subtaskId;
    private final int numberOfSubtasks;

    CommittableCollector(int subtaskId, int numberOfSubtasks) {
        this.subtaskId = subtaskId;
        this.numberOfSubtasks = numberOfSubtasks;
        checkpointCommittables = new TreeMap<>();
    }

    /** For deserialization. */
    CommittableCollector(Map<Long, Checkpoint<CommT>> checkpointCommittables) {
        this.checkpointCommittables = new TreeMap<>(checkpointCommittables);
        this.subtaskId = 0;
        this.numberOfSubtasks = 1;
    }

    public static <CommT> CommittableCollector<CommT> of(RuntimeContext context) {
        return new CommittableCollector<>(
                context.getIndexOfThisSubtask(), context.getNumberOfParallelSubtasks());
    }

    static <CommT> CommittableCollector<CommT> ofLegacy(List<CommT> r) {
        CommittableCollector<CommT> committableCollector = new CommittableCollector<>(0, 1);
        // add a checkpoint with the lowest checkpoint id, this will be merged into the next
        // checkpoint data, subtask id is arbitrary
        CommittableSummary<CommT> summary =
                new CommittableSummary<>(
                        0, 1, InitContext.INITIAL_CHECKPOINT_ID, r.size(), r.size());
        committableCollector.addSummary(summary);
        Subtask<CommT> subtask =
                committableCollector
                        .getCheckpointCommittables(summary)
                        .getSubtaskCommittables(summary.getSubtaskId());
        r.forEach(subtask::add);
        return committableCollector;
    }

    public void addMessage(CommittableMessage<CommT> message) {
        if (message instanceof CommittableSummary) {
            addSummary((CommittableSummary<CommT>) message);
        } else if (message instanceof CommittableWithLineage) {
            addCommittable((CommittableWithLineage<CommT>) message);
        }
    }

    public void addSummary(CommittableSummary<CommT> summary) {
        checkpointCommittables
                .computeIfAbsent(
                        summary.getCheckpointId().orElse(EOI),
                        key -> new Checkpoint<>(this, summary.getCheckpointId().orElse(EOI)))
                .addSummary(summary);
    }

    public void addCommittable(CommittableWithLineage<CommT> committable) {
        getCheckpointCommittables(committable).addCommittable(committable);
    }

    private Checkpoint<CommT> getCheckpointCommittables(CommittableMessage<CommT> committable) {
        Checkpoint<CommT> committables =
                this.checkpointCommittables.get(committable.getCheckpointId().orElse(EOI));
        return checkNotNull(committables, "Unknown checkpoint for %s", committable);
    }

    public Collection<? extends CheckpointCommittables<CommT>> getCheckpointCommittablesUpTo(
            long checkpointId) {
        // clean up fully committed previous checkpoints
        // this wouldn't work with concurrent unaligned checkpoints
        Collection<Checkpoint<CommT>> checkpoints =
                checkpointCommittables.headMap(checkpointId, true).values();
        checkpoints.removeIf(Checkpoint::isFinished);
        return checkpoints;
    }

    @Nullable
    public Committables<CommT> getEndOfInputCommittables() {
        return checkpointCommittables.get(EOI);
    }

    Collection<Checkpoint<CommT>> getCheckpoints() {
        return checkpointCommittables.values();
    }

    public boolean isFinished() {
        return checkpointCommittables.values().stream().allMatch(Checkpoint::isFinished);
    }

    public void merge(CommittableCollector<CommT> cc) {
        for (Entry<Long, Checkpoint<CommT>> checkpointEntry :
                cc.checkpointCommittables.entrySet()) {
            checkpointCommittables.merge(
                    checkpointEntry.getKey(), checkpointEntry.getValue(), Checkpoint::merge);
        }
    }

    static class Checkpoint<CommT> implements CheckpointCommittables<CommT> {
        private final Map<Integer, Subtask<CommT>> subtaskCommittables;
        private final CommittableCollector<CommT> collector;
        private final long checkpointId;

        Checkpoint(CommittableCollector<CommT> collector, long checkpointId) {
            this.collector = collector;
            this.checkpointId = checkpointId;
            subtaskCommittables = new HashMap<>();
        }

        Checkpoint(Map<Integer, Subtask<CommT>> subtaskCommittables, long checkpointId) {
            this.subtaskCommittables = subtaskCommittables;
            // dummy collector, never used
            this.collector = new CommittableCollector<>(0, 1);
            this.checkpointId = checkpointId;
        }

        @Override
        public long getCheckpointId() {
            return checkpointId;
        }

        Collection<Subtask<CommT>> getSubtasks() {
            return subtaskCommittables.values();
        }

        void addSummary(CommittableSummary<CommT> summary) {
            Subtask<CommT> existing =
                    subtaskCommittables.putIfAbsent(summary.getSubtaskId(), new Subtask<>(summary));
            if (existing != null) {
                // there has been an update coming in, this may have increased the number of
                // committables/pending, so let's update
                existing.summary = summary;
            }
        }

        void addCommittable(CommittableWithLineage<CommT> committable) {
            getSubtaskCommittables(committable.getSubtaskId()).add(committable);
        }

        Subtask<CommT> getSubtaskCommittables(int subtaskId) {
            Subtask<CommT> committables = this.subtaskCommittables.get(subtaskId);
            return checkNotNull(committables, "Unknown subtask for %s", subtaskId);
        }

        @Override
        public CommittableSummary<CommT> getSummary() {
            return new CommittableSummary<>(
                    collector.subtaskId,
                    collector.numberOfSubtasks,
                    checkpointId,
                    subtaskCommittables.values().stream()
                            .mapToInt(Subtask::getNumCommittables)
                            .sum(),
                    subtaskCommittables.values().stream().mapToInt(Subtask::getNumPending).sum());
        }

        boolean isFinished() {
            return subtaskCommittables.values().stream().allMatch(Subtask::isFinished);
        }

        @Override
        public Collection<CommittableWithLineage<CommT>> commit(
                boolean fullyReceived, Committer<CommT> committer)
                throws IOException, InterruptedException {
            Collection<Request<CommT>> requests = getPendingRequests(fullyReceived);
            requests.forEach(Request::setSelected);
            committer.commit(new ArrayList<>(requests));
            requests.forEach(Request::setCommittedIfNoError);
            return drainFinished();
        }

        Collection<Request<CommT>> getPendingRequests(boolean fullyReceived) {
            return subtaskCommittables.values().stream()
                    .filter(subtask -> !fullyReceived || subtask.hasReceivedAll())
                    .flatMap(Subtask::getPendingRequests)
                    .collect(Collectors.toList());
        }

        Collection<CommittableWithLineage<CommT>> drainFinished() {
            return subtaskCommittables.values().stream()
                    .flatMap(subtask -> subtask.drainCommitted().stream())
                    .collect(Collectors.toList());
        }

        Checkpoint<CommT> merge(Checkpoint<CommT> other) {
            checkArgument(other.checkpointId == checkpointId);
            for (Entry<Integer, Subtask<CommT>> subtaskEntry :
                    other.subtaskCommittables.entrySet()) {
                subtaskCommittables.merge(
                        subtaskEntry.getKey(), subtaskEntry.getValue(), Subtask::merge);
            }
            return this;
        }
    }

    static class Subtask<CommT> {
        private CommittableSummary<CommT> summary;
        private final Deque<Request<CommT>> requests;
        private int numDrained;

        private Subtask(CommittableSummary<CommT> summary) {
            this.summary = checkNotNull(summary);
            requests = new ArrayDeque<>(summary.getNumberOfCommittables());
        }

        Subtask(
                CommittableSummary<CommT> summary,
                Collection<Request<CommT>> requests,
                int numDrained) {
            this.summary = checkNotNull(summary);
            this.requests = new ArrayDeque<>(requests);
            this.numDrained = numDrained;
        }

        void add(CommittableWithLineage<CommT> committable) {
            add(committable.getCommittable());
        }

        void add(CommT committable) {
            requests.add(new Request<>(committable));
        }

        boolean hasReceivedAll() {
            return getNumCommittables() == summary.getNumberOfCommittables();
        }

        private int getNumCommittables() {
            return requests.size() + numDrained;
        }

        private int getNumPending() {
            return summary.getNumberOfCommittables() - numDrained;
        }

        boolean isFinished() {
            return getNumPending() == 0;
        }

        Stream<Request<CommT>> getPendingRequests() {
            return requests.stream().filter(c -> !c.isFinished());
        }

        List<CommittableWithLineage<CommT>> drainCommitted() {
            List<CommittableWithLineage<CommT>> committed = new ArrayList<>(requests.size());
            for (Iterator<Request<CommT>> iterator = requests.iterator(); iterator.hasNext(); ) {
                Request<CommT> request = iterator.next();
                if (request.isFinished()) {
                    committed.add(
                            new CommittableWithLineage<>(
                                    request.getCommittable(),
                                    summary.getCheckpointId().isPresent()
                                            ? summary.getCheckpointId().getAsLong()
                                            : null,
                                    summary.getSubtaskId()));
                    iterator.remove();
                }
            }

            numDrained += committed.size();
            return committed;
        }

        CommittableSummary<CommT> getSummary() {
            return summary;
        }

        int getNumDrained() {
            return numDrained;
        }

        Deque<Request<CommT>> getRequests() {
            return requests;
        }

        public Subtask<CommT> merge(Subtask<CommT> other) {
            CommittableSummary<CommT> otherSummary = other.getSummary();
            checkArgument(otherSummary.getSubtaskId() == this.summary.getSubtaskId());
            OptionalLong checkpointId = this.summary.getCheckpointId();
            this.summary =
                    new CommittableSummary<>(
                            this.summary.getSubtaskId(),
                            this.summary.getNumberOfCommittables(),
                            checkpointId.isPresent() ? checkpointId.getAsLong() : null,
                            this.summary.getNumberOfCommittables()
                                    + otherSummary.getNumberOfCommittables(),
                            this.summary.getNumberOfPendingCommits()
                                    + otherSummary.getNumberOfPendingCommits());
            this.requests.addAll(other.requests);
            this.numDrained += other.numDrained;
            return this;
        }
    }

    @Internal
    public enum RequestState {
        RECEIVED(false),
        RETRY(false),
        FAILED(true),
        COMMITTED(true);

        final boolean finalState;

        RequestState(boolean finalState) {
            this.finalState = finalState;
        }

        public boolean isFinalState() {
            return finalState;
        }
    }

    @Internal
    public static class Request<CommT> implements CommitRequest<CommT> {

        private CommT committable;
        private int numRetries;
        private RequestState state;

        protected Request(CommT committable) {
            this.committable = committable;
            state = RequestState.RECEIVED;
        }

        protected Request(CommT committable, int numRetries, RequestState state) {
            this.committable = committable;
            this.numRetries = numRetries;
            this.state = state;
        }

        boolean isFinished() {
            return state.isFinalState();
        }

        RequestState getState() {
            return state;
        }

        @Override
        public CommT getCommittable() {
            return committable;
        }

        @Override
        public int getNumberOfRetries() {
            return numRetries;
        }

        @Override
        public void failedWithKnownReason() {
            state = RequestState.FAILED;
            // add metric later
            // let the user configure a strategy for failing and apply it here
            throw new IllegalStateException("Failed to commit " + committable);
        }

        @Override
        public void failedWithUnknownReason() {
            state = RequestState.FAILED;
            // add metric later
            // let the user configure a strategy for failing and apply it here
            throw new IllegalStateException("Failed to commit " + committable);
        }

        @Override
        public void retryLater() {
            state = RequestState.RETRY;
            numRetries++;
            // add metric later
        }

        @Override
        public void updateAndRetryLater(CommT committable) {
            this.committable = committable;
            retryLater();
        }

        @Override
        public void alreadyCommitted() {
            // add metric later
            state = RequestState.COMMITTED;
        }

        void setSelected() {
            state = RequestState.RECEIVED;
        }

        void setCommittedIfNoError() {
            if (state == RequestState.RECEIVED) {
                state = RequestState.COMMITTED;
            }
        }
    }
}
