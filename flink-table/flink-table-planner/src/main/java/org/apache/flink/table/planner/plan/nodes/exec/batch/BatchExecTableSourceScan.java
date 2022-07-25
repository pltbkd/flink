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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.InputFormatSourceFunction;
import org.apache.flink.streaming.api.transformations.MultipleInputTransformation;
import org.apache.flink.streaming.api.transformations.SourceTransformation;
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
import org.apache.flink.table.planner.plan.rules.physical.batch.RuntimeFilterRule.BatchExecRuntimeFilterBuilder;
import org.apache.flink.table.runtime.operators.dpp.DppFilterOperatorFactory;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableFunction;

import org.apache.calcite.util.mapping.IntPair;

import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Batch {@link ExecNode} to read data from an external source defined by a bounded {@link
 * ScanTableSource}.
 */
public class BatchExecTableSourceScan extends CommonExecTableSourceScan
        implements BatchExecNode<RowData> {
    private BatchExecDynamicPartitionSink cachedDppSink;

    private boolean skipDependencyEdge;

    private boolean hasDppSink;
    private boolean hasDf;

    private String dfUUID;
    private List<IntPair> joinKeys;

    public BatchExecTableSourceScan(
            ReadableConfig tableConfig,
            DynamicTableSourceSpec tableSourceSpec,
            RowType outputType,
            String description,
            boolean hasDppSink,
            boolean hasDf,
            String dfUUID,
            List<IntPair> joinKeys) {
        super(
                ExecNodeContext.newNodeId(),
                ExecNodeContext.newContext(BatchExecTableSourceScan.class),
                ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class, tableConfig),
                tableSourceSpec,
                outputType,
                description,
                hasDppSink || hasDf
                        ? Collections.singletonList(InputProperty.DEFAULT)
                        : Collections.emptyList());
        this.hasDppSink = hasDppSink;
        this.hasDf = hasDf;
        if (hasDf) {
            this.dfUUID = checkNotNull(dfUUID);
            this.joinKeys = checkNotNull(joinKeys);
        }
    }

    public void setSkipDependencyEdge(boolean skipDependencyEdge) {
        this.skipDependencyEdge = skipDependencyEdge;
    }

    public void setCachedDppSink(BatchExecDynamicPartitionSink cachedDppSink) {
        this.cachedDppSink = cachedDppSink;
    }

    @Override
    protected Transformation<RowData> translateToPlanInternal(
            PlannerBase planner, ExecNodeConfig config) {
        Transformation<RowData> transformation = super.translateToPlanInternal(planner, config);
        // the boundedness has been checked via the runtime provider already, so we can safely
        // declare all legacy transformations as bounded to make the stream graph generator happy
        ExecNodeUtil.makeLegacySourceTransformationsBounded(transformation);

        System.out.println("Translate " + this + ", skipDependencyEdge = " + skipDependencyEdge);

        List<ExecEdge> edges = getInputEdges();
        if (edges.size() == 0) {
            // Case 1. No input edges, the dynamic sink might not exists or created via roots of
            // exec graph

            if (cachedDppSink != null) {
                String partitionDataListenerID = UUID.randomUUID().toString();
                ((SourceTransformation<?, ?, ?>) transformation)
                        .setCoordinatorListeningID(partitionDataListenerID);
                cachedDppSink.registerPartitionDataListenerID(
                        String.valueOf(transformation.getId()));
                Transformation<Object> dppTransformation = cachedDppSink.translateToPlan(planner);
                planner.addExtraTransformation(dppTransformation);
            }

            return transformation;
        }

        if (hasDppSink) {
            BatchExecDynamicPartitionSink sink =
                    (BatchExecDynamicPartitionSink) edges.get(0).getSource();
            String partitionDataListenerID = UUID.randomUUID().toString();
            ((SourceTransformation<?, ?, ?>) transformation)
                    .setCoordinatorListeningID(partitionDataListenerID);
            sink.registerPartitionDataListenerID(partitionDataListenerID);
            Transformation<Object> dppTransformation = sink.translateToPlan(planner);

            if (skipDependencyEdge) {
                // Case 2. source -> Multiple without exchange, this should not happen in fact.
                planner.addExtraTransformation(dppTransformation);
                return transformation;
            }

            // Case 3. The dependency edge is required.
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

        if (hasDf) {
            BatchExecRuntimeFilterBuilder runtimeFilterBuilder =
                    (BatchExecRuntimeFilterBuilder) edges.get(0).getSource();
            final List<Tuple2<Integer, Integer>> keys =
                    joinKeys.stream()
                            .map(p -> new Tuple2<>(p.source, p.target))
                            .collect(Collectors.toList());
            final RowType rowType = (RowType) getOutputType();
            ((SourceTransformation<?, ?, ?>) transformation)
                    .withRuntimeFilter(
                            this.dfUUID,
                            (SerializableFunction<Object, Object[]>)
                                    o -> {
                                        assert o instanceof RowData;
                                        RowData row = (RowData) o;
                                        Object[] values = new Object[keys.size()];
                                        for (int i = 0; i < values.length; i++) {
                                            int index = keys.get(i).f1;
                                            LogicalType type = rowType.getTypeAt(index);
                                            switch (type.getTypeRoot()) {
                                                case INTEGER:
                                                    values[i] = row.getInt(index);
                                                    break;
                                                case BIGINT:
                                                    values[i] = row.getLong(index);
                                                    break;
                                                case VARCHAR:
                                                    values[i] = row.getString(index).toString();
                                                    break;
                                                default:
                                                    // TODO more types?
                                                    throw new UnsupportedOperationException();
                                            }
                                        }
                                        return values;
                                    });
            Transformation<Object> dfTransformation = runtimeFilterBuilder.translateToPlan(planner);

            if (skipDependencyEdge) {
                // Case 2. source -> Multiple without exchange, this should not happen in fact.
                planner.addExtraTransformation(dfTransformation);
                return transformation;
            }

            // Case 3. The dependency edge is required.
            MultipleInputTransformation<RowData> multipleInputTransformation =
                    new MultipleInputTransformation<>(
                            "Placeholder-Filter",
                            new DppFilterOperatorFactory(),
                            transformation.getOutputType(),
                            transformation.getParallelism());
            multipleInputTransformation.addInput(dfTransformation);
            multipleInputTransformation.addInput(transformation);

            return multipleInputTransformation;
        }

        throw new IllegalStateException("Source has unexpected input:" + edges.get(0).toString());
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
