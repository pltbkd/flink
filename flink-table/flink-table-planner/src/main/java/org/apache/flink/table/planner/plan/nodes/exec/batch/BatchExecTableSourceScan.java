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

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.planner.delegation.PlannerBase;
import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeConfig;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeContext;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.common.CommonExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec;
import org.apache.flink.table.planner.plan.nodes.exec.utils.ExecNodeUtil;
import org.apache.flink.table.runtime.operators.dpp.DppFilterOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.RowType;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * ScanTableSource}.
 */
public class BatchExecTableSourceScan extends CommonExecTableSourceScan
        implements BatchExecNode<RowData> {

    private final String partitionDataListenerID;

    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            RowType outputType,
            String description,
            String partitionDataListenerID,
            boolean hasDppSink) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                outputType,
                description,
                hasDppSink
                        ? Collections.singletonList(InputProperty.DEFAULT)
                        : Collections.emptyList());
        this.partitionDataListenerID = partitionDataListenerID;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> transformation = super.translateToPlanInternal(planner, config);
        // the boundedness has been checked via the runtime provider already, so we can safely
        // declare all legacy transformations as bounded to make the stream graph generator happy
        ExecNodeUtil.makeLegacySourceTransformationsBounded(transformation);

        List<ExecEdge> edges = getInputEdges();
        if (edges.size() == 0) {
            return transformation;
        }

        checkState(
                partitionDataListenerID != null,
                "Dynamic partition pruning is not supported since partition data listener is not identified.");
        BatchExecDynamicPartitionSink sink =
                (BatchExecDynamicPartitionSink) edges.get(0).getSource();
        sink.registerPartitionDataListenerID(partitionDataListenerID);
        Transformation<Object> dppTransformation = sink.translateToPlan(planner);

        MultipleInputTransformation<RowData> multipleInputTransformation =
                new MultipleInputTransformation<>(
                        "Placeholder-Filter",
                        new DppFilterOperatorFactory(),
                        transformation.getOutputType(),
                        transformation.getParallelism());
        multipleInputTransformation.addInput(dppTransformation);
        multipleInputTransformation.addInput(transformation);

        return multipleInputTransformation;
    }

    @Override
    public Transformation<RowData> createInputFormatTransformation(
            StreamExecutionEnvironment env,
            InputFormat<RowData, ?> inputFormat,
            InternalTypeInfo<RowData> outputTypeInfo,
            String operatorName) {
        // env.createInput will use ContinuousFileReaderOperator, but it do not support multiple
        // paths. If read partitioned source, after partition pruning, we need let InputFormat
        // to read multiple partitions which are multiple paths.
        // We can use InputFormatSourceFunction directly to support InputFormat.
        final InputFormatSourceFunction<RowData> function =
                new InputFormatSourceFunction<>(inputFormat, outputTypeInfo);
        return env.addSource(function, operatorName, outputTypeInfo).getTransformation();
    }
}
