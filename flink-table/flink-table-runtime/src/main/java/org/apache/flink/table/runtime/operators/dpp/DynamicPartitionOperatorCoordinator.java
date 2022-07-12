/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.runtime.operators.dpp;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.CoordinationRequest;
import org.apache.flink.runtime.operators.coordination.CoordinationRequestHandler;
import org.apache.flink.runtime.operators.coordination.CoordinationResponse;
import org.apache.flink.runtime.operators.coordination.CoordinatorStore;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;
import org.apache.flink.table.connector.source.PartitionData;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

/** DynamicPartitionSinkOperatorCoordinator. */
public class DynamicPartitionOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {

    private final Context context;
    private final String coordinatingMailboxID;
    private CompletableFuture<PartitionData> partitionDataFuture;

    public DynamicPartitionOperatorCoordinator(Context context, String coordinatingMailboxID) {
        this.context = context;
        this.coordinatingMailboxID = coordinatingMailboxID;
    }

    @Override
    @SuppressWarnings("unchecked")
    public void start() throws Exception {
        CoordinatorStore coordinatorStore = context.getCoordinatorStore();
        synchronized (coordinatorStore) {
            coordinatorStore.putIfAbsent(
                    coordinatingMailboxID, new CompletableFuture<PartitionData>());
            partitionDataFuture =
                    (CompletableFuture<PartitionData>) coordinatorStore.get(coordinatingMailboxID);
        }
    }

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        this.partitionDataFuture.complete(((DynamicPartitionEvent) event).getData());
    }

    @Override
    public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
            CoordinationRequest request) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        // subtask failed, the socket server does not exist anymore
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        // nothing to do here, connections are re-created lazily
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> result)
            throws Exception {}

    @Override
    public void notifyCheckpointComplete(long checkpointId) {}

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {}

    /** Provider for {@link DynamicPartitionOperatorCoordinator}. */
    public static class Provider implements OperatorCoordinator.Provider {

        private final OperatorID operatorID;
        private final String coordinatingMailboxID;

        public Provider(OperatorID operatorID, String coordinatingMailboxID) {
            this.operatorID = operatorID;
            this.coordinatingMailboxID = coordinatingMailboxID;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new DynamicPartitionOperatorCoordinator(context, coordinatingMailboxID);
        }
    }
}
