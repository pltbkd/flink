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

package org.apache.flink.table.planner.plan.nodes.exec.batch;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.operators.SimpleOperatorFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.dpp.DynamicPartitionSinkFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.CompletableFuture;

import static org.apache.flink.util.Preconditions.checkArgument;

/** BatchExecDynamicPartitionSink. */
public class BatchExecDynamicPartitionSink extends ExecNodeBase<Object>
        implements BatchExecNode<Object> {

    private final List<Integer> partitionFields;
    private transient CompletableFuture<byte[]> sourceOperatorIdFuture;

    public BatchExecDynamicPartitionSink(
            List<Integer> partitionFields,
            ReadableConfig tableConfig,
            InputProperty inputProperty,
            RowType outputType,
            String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                Collections.singletonList(inputProperty),
                outputType,
                description);
        this.partitionFields = partitionFields;
        checkArgument(outputType.getFieldCount() == partitionFields.size());
    }

    public void setSourceOperatorIdFuture(CompletableFuture<byte[]> sourceOperatorIdFuture) {
        this.sourceOperatorIdFuture = sourceOperatorIdFuture;
    }

    @Override
    @SuppressWarnings("unchecked")
    protected Transformation<Object> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        final ExecEdge inputEdge = getInputEdges().get(0);
        final Transformation<RowData> inputTransform =
                (Transformation<RowData>) inputEdge.translateToPlan(planner);
        SimpleOperatorFactory<Object> factory =
                new DynamicPartitionSinkFactory(
                        sourceOperatorIdFuture, (RowType) getOutputType(), partitionFields);

        return ExecNodeUtil.createOneInputTransformation(
                inputTransform,
                createTransformationName(config),
                createTransformationDescription(config),
                factory,
                InternalTypeInfo.of(getOutputType()),
                1); // parallelism should always be 1
    }
}
