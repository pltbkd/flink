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
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.runtime.operators.dpp.DppFilterOperatorFactory;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;

public class BatchExecDynamicPartitionPlaceholderFilter extends ExecNodeBase<RowData>
        implements BatchExecNode<RowData> {

    public BatchExecDynamicPartitionPlaceholderFilter(
            ReadableConfig tableConfig, RowType outputType, String description) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                Arrays.asList(InputProperty.DEFAULT, InputProperty.DEFAULT),
                outputType,
                description);
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        List<ExecEdge> edges = getInputEdges();

        Transformation<RowData> sourceTransformation =
                (Transformation<RowData>) edges.get(1).getSource().translateToPlan(planner);
        CompletableFuture<byte[]> sourceOperatorIdFuture =
                ((SourceTransformation<?, ?, ?>) sourceTransformation)
                        .getSource()
                        .getOperatorIdFuture();

        ((BatchExecDynamicPartitionSink) edges.get(0).getSource())
                .setSourceOperatorIdFuture(sourceOperatorIdFuture);
        Transformation<Object> dppTransformation =
                (Transformation<Object>) edges.get(0).getSource().translateToPlan(planner);

        MultipleInputTransformation<RowData> multipleInputTransformation =
                new MultipleInputTransformation<>(
                        "Filter",
                        new DppFilterOperatorFactory(),
                        sourceTransformation.getOutputType(),
                        sourceTransformation.getParallelism());
        multipleInputTransformation.addInput(dppTransformation);
        multipleInputTransformation.addInput(sourceTransformation);

        return multipleInputTransformation;
    }
}
