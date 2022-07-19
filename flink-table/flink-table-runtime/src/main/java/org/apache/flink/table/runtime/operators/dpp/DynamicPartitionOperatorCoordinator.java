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

import javax.annotation.Nullable;

import java.util.List;
import java.util.concurrent.CompletableFuture;

/** DynamicPartitionSinkOperatorCoordinator. */
public class DynamicPartitionOperatorCoordinator
        implements OperatorCoordinator, CoordinationRequestHandler {

    private final CoordinatorStore coordinatorStore;
    private final List<String> partitionDataListenerIDs;

    public DynamicPartitionOperatorCoordinator(
            Context context, List<String> partitionDataListenerIDs) {
        this.coordinatorStore = context.getCoordinatorStore();
        this.partitionDataListenerIDs = partitionDataListenerIDs;
    }

    @Override
    public void start() throws Exception {}

    @Override
    public void close() throws Exception {}

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        for (String listenerID : partitionDataListenerIDs) {
            // push event
            OperatorCoordinator listener = (OperatorCoordinator) coordinatorStore.get(listenerID);
            if (listener == null) {
                throw new IllegalStateException("Partition data listener missing");
            }
            listener.handleEventFromOperator(0, event);
        }
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
        private final List<String> partitionDataListenerIDs;

        public Provider(OperatorID operatorID, List<String> partitionDataListenerIDs) {
            this.operatorID = operatorID;
            this.partitionDataListenerIDs = partitionDataListenerIDs;
        }

        @Override
        public OperatorID getOperatorId() {
            return operatorID;
        }

        @Override
        public OperatorCoordinator create(Context context) {
            return new DynamicPartitionOperatorCoordinator(context, partitionDataListenerIDs);
        }
    }
}
