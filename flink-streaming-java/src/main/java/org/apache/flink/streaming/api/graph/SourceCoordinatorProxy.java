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

package org.apache.flink.streaming.api.graph;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEvent;

import javax.annotation.Nullable;

import java.util.concurrent.CompletableFuture;

public class SourceCoordinatorProxy implements OperatorCoordinator {
    private final Context context;
    private final int transformationID;
    private final OperatorCoordinator operatorCoordinator;

    public SourceCoordinatorProxy(
            Context context, int transformationID, OperatorCoordinator operatorCoordinator) {
        this.context = context;
        this.transformationID = transformationID;
        this.operatorCoordinator = operatorCoordinator;

        context.getCoordinatorStore().putIfAbsent(String.valueOf(transformationID), this);
    }

    @Override
    public void start() throws Exception {
        operatorCoordinator.start();
    }

    @Override
    public void close() throws Exception {
        operatorCoordinator.close();
    }

    @Override
    public void handleEventFromOperator(int subtask, OperatorEvent event) throws Exception {
        operatorCoordinator.handleEventFromOperator(subtask, event);
    }

    @Override
    public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]> resultFuture)
            throws Exception {
        operatorCoordinator.checkpointCoordinator(checkpointId, resultFuture);
    }

    @Override
    public void notifyCheckpointComplete(long checkpointId) {
        operatorCoordinator.notifyCheckpointComplete(checkpointId);
    }

    @Override
    public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
            throws Exception {
        operatorCoordinator.resetToCheckpoint(checkpointId, checkpointData);
    }

    @Override
    public void subtaskFailed(int subtask, @Nullable Throwable reason) {
        operatorCoordinator.subtaskFailed(subtask, reason);
    }

    @Override
    public void subtaskReset(int subtask, long checkpointId) {
        operatorCoordinator.subtaskReset(subtask, checkpointId);
    }

    @Override
    public void subtaskReady(int subtask, SubtaskGateway gateway) {
        operatorCoordinator.subtaskReady(subtask, gateway);
    }

    public static class SourceCoordinatorProxyProvider implements OperatorCoordinator.Provider {

        private final int transformationID;
        private final OperatorCoordinator.Provider provider;

        public SourceCoordinatorProxyProvider(
                int transformationID, OperatorCoordinator.Provider provider) {
            this.transformationID = transformationID;
            this.provider = provider;
        }

        @Override
        public OperatorID getOperatorId() {
            return provider.getOperatorId();
        }

        @Override
        public OperatorCoordinator create(Context context) throws Exception {
            return new SourceCoordinatorProxy(context, transformationID, provider.create(context));
        }
    }
}
