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

package org.apache.flink.table.runtime.operators.dpp;

import org.apache.flink.runtime.jobgraph.OperatorID;
import org.apache.flink.runtime.operators.coordination.OperatorCoordinator;
import org.apache.flink.runtime.operators.coordination.OperatorEventDispatcher;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.streaming.api.operators.CoordinatedOperatorFactory;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.types.logical.RowType;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;

/** DynamicPartitionSinkFactory. */
public class DynamicPartitionOperatorFactory extends SimpleOperatorFactory<Object>
        implements CoordinatedOperatorFactory<Object> {

    private final transient List<CompletableFuture<byte[]>> sourceOperatorIdFutures;
    private List<OperatorID> sourceOperatorIds = new ArrayList<>();
    private final DynamicPartitionOperator operator;

    public DynamicPartitionOperatorFactory(
            List<CompletableFuture<byte[]>> sourceOperatorIdFutures,
            RowType partitionFieldType,
            List<Integer> partitionFieldIndices) {
        super(new DynamicPartitionOperator(partitionFieldType, partitionFieldIndices));
        this.operator = (DynamicPartitionOperator) getOperator();
        this.sourceOperatorIdFutures = sourceOperatorIdFutures;
    }

    @Override
    public <T extends StreamOperator<Object>> T createStreamOperator(
            StreamOperatorParameters<Object> parameters) {
        final OperatorEventDispatcher eventDispatcher = parameters.getOperatorEventDispatcher();

        List<OperatorEventGateway> gateways = new ArrayList<>();
        sourceOperatorIds.forEach(
                sourceOperatorId -> {
                    if (sourceOperatorId == null) {
                        throw new TableException("sourceOperatorId is empty");
                    }

                    gateways.add(eventDispatcher.getOperatorEventGateway(sourceOperatorId));
                });

        operator.setOperatorEventGateways(gateways);

        operator.setup(
                parameters.getContainingTask(),
                parameters.getStreamConfig(),
                parameters.getOutput());

        // today's lunch is generics spaghetti
        @SuppressWarnings("unchecked")
        final T castedOperator = (T) operator;

        return castedOperator;
    }

    @Override
    public OperatorCoordinator.Provider getCoordinatorProvider(
            String operatorName, OperatorID operatorID) {

        sourceOperatorIdFutures.forEach(
                sourceOperatorIdFuture -> {
                    byte[] sourceOperatorIdBytes = sourceOperatorIdFuture.getNow(null);
                    if (sourceOperatorIdBytes == null) {
                        throw new TableException("sourceOperatorId is empty");
                    }
                    sourceOperatorIds.add(new OperatorID(sourceOperatorIdBytes));
                });

        return new DynamicPartitionOperatorCoordinator.Provider(operatorID);
    }
}
