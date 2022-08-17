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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArrayComparator;
import org.apache.flink.api.connector.source.RuntimeFilter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataOutputViewStreamWrapper;
import org.apache.flink.core.memory.MemorySegment;
import org.apache.flink.core.memory.MemorySegmentFactory;
import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.operators.util.BloomFilter;
import org.apache.flink.runtime.source.coordinator.BloomRuntimeFilter;
import org.apache.flink.runtime.source.coordinator.RuntimeFilterEvent;
import org.apache.flink.runtime.source.coordinator.RuntimeFilterEvent.ReceiverType;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.DynamicFilteringData;
import org.apache.flink.table.connector.source.DynamicFilteringDataRuntimeFilter;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.runtime.typeutils.InternalTypeInfo;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.function.SerializableFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Operator to collect and build the {@link DynamicFilteringData} for sources that supports dynamic
 * filtering.
 */
public class DynamicFilteringDataCollectorOperator extends AbstractStreamOperator<Object>
        implements OneInputStreamOperator<RowData, Object> {

    private static final Logger LOG =
            LoggerFactory.getLogger(DynamicFilteringDataCollectorOperator.class);

    private final OperatorEventGateway operatorEventGateway;

    private final List<FilterConfig> filterConfigs;
    private final long rowDataThreshold;
    private final RowType inputType;

    private transient List<FilterBuilder> filterBuilders;

    public enum RuntimeFilterType {
        ROW_DATA,
        BLOOM
    }

    public static class FilterConfig implements Serializable {
        String receiverUUID;
        int[] dimIndices;
        int[] factIndices;
        RuntimeFilterType type;

        RuntimeFilterEvent.ReceiverType receiverType;

        public FilterConfig(
                String receiverUUID,
                int[] dimIndices,
                int[] factIndices,
                RuntimeFilterType type,
                RuntimeFilterEvent.ReceiverType receiverType) {
            this.receiverUUID = receiverUUID;
            this.dimIndices = dimIndices;
            this.factIndices = factIndices;
            this.type = type;
            this.receiverType = receiverType;
        }

        @Override
        public String toString() {
            return "FilterConfig{"
                    + "receiverUUID="
                    + receiverUUID
                    + ", dimIndices="
                    + Arrays.toString(dimIndices)
                    + ", factIndices="
                    + Arrays.toString(factIndices)
                    + ", type="
                    + type
                    + ", receiverType="
                    + receiverType
                    + '}';
        }
    }

    public DynamicFilteringDataCollectorOperator(
            RowType inputType,
            List<FilterConfig> filterConfigs,
            long rowDataThreshold,
            OperatorEventGateway operatorEventGateway) {
        this.inputType = inputType;
        this.filterConfigs = checkNotNull(filterConfigs);
        this.rowDataThreshold = rowDataThreshold;
        this.operatorEventGateway = checkNotNull(operatorEventGateway);
    }

    @Override
    public void open() throws Exception {
        super.open();
        // TODO missing estimatedBuildCount, provider instead of config?
        int estimatedBuildCount = 100000;
        this.filterBuilders =
                filterConfigs.stream()
                        .map(
                                conf ->
                                        conf.type == RuntimeFilterType.ROW_DATA
                                                ? new DynamicFilteringDataFilterBuilder(
                                                        inputType,
                                                        Arrays.stream(conf.dimIndices)
                                                                .boxed()
                                                                .collect(Collectors.toList()),
                                                        rowDataThreshold)
                                                : new BloomFilterBuilder(
                                                        inputType,
                                                        estimatedBuildCount,
                                                        IntStream.range(0, conf.dimIndices.length)
                                                                .mapToObj(
                                                                        i ->
                                                                                new Tuple2<>(
                                                                                        conf.factIndices[
                                                                                                i],
                                                                                        conf.dimIndices[
                                                                                                i]))
                                                                .collect(Collectors.toList())))
                        .collect(Collectors.toList());
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        RowData value = element.getValue();
        for (FilterBuilder filterBuilder : filterBuilders) {
            filterBuilder.add(value);
        }
    }

    @Override
    public void finish() throws Exception {
        for (int i = 0; i < filterBuilders.size(); i++) {
            String receiverUUID = filterConfigs.get(i).receiverUUID;
            ReceiverType receiverType = filterConfigs.get(i).receiverType;
            FilterBuilder filterBuilder = filterBuilders.get(i);
            RuntimeFilterEvent<RowData> event =
                    new RuntimeFilterEvent<>(receiverUUID, receiverType, filterBuilder.build());
            operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
        }
    }

    @Override
    public void close() throws Exception {
        super.close();
        //        if (buffer != null) {
        //            buffer.clear();
        //        }
    }

    private interface FilterBuilder {

        void add(RowData value) throws IOException;

        RuntimeFilter<RowData> build();
    }

    private static class DynamicFilteringDataFilterBuilder implements FilterBuilder {

        private final RowType inputType;
        private final List<Integer> dynamicFilteringFieldIndices;
        private final long rowDataThreshold;

        private final RowType projectedType;
        private transient TypeInformation<RowData> typeInfo;
        private transient TypeSerializer<RowData> serializer;

        private transient Set<byte[]> buffer;
        private transient long currentSize;
        private transient FieldGetter[] fieldGetters;

        public DynamicFilteringDataFilterBuilder(
                RowType inputType,
                List<Integer> dynamicFilteringFieldIndices,
                long rowDataThreshold) {
            this.inputType = checkNotNull(inputType);
            this.dynamicFilteringFieldIndices = checkNotNull(dynamicFilteringFieldIndices);
            this.rowDataThreshold = rowDataThreshold;

            this.projectedType =
                    RowType.of(
                            dynamicFilteringFieldIndices.stream()
                                    .map(inputType::getTypeAt)
                                    .toArray(LogicalType[]::new),
                            dynamicFilteringFieldIndices.stream()
                                    .map(i -> inputType.getFieldNames().get(i))
                                    .toArray(String[]::new));
            this.typeInfo = InternalTypeInfo.of(projectedType);
            this.serializer = typeInfo.createSerializer(new ExecutionConfig());
            this.buffer = new TreeSet<>(new BytePrimitiveArrayComparator(true)::compare);
            this.currentSize = 0L;
            this.fieldGetters =
                    IntStream.range(0, dynamicFilteringFieldIndices.size())
                            .mapToObj(
                                    i ->
                                            RowData.createFieldGetter(
                                                    inputType.getTypeAt(
                                                            dynamicFilteringFieldIndices.get(i)),
                                                    dynamicFilteringFieldIndices.get(i)))
                            .toArray(FieldGetter[]::new);
        }

        @Override
        public void add(RowData value) throws IOException {
            if (exceedThreshold()) {
                return;
            }

            GenericRowData rowData = new GenericRowData(dynamicFilteringFieldIndices.size());
            for (int i = 0; i < dynamicFilteringFieldIndices.size(); ++i) {
                rowData.setField(i, fieldGetters[i].getFieldOrNull(value));
            }

            try (ByteArrayOutputStream baos = new ByteArrayOutputStream()) {
                DataOutputViewStreamWrapper wrapper = new DataOutputViewStreamWrapper(baos);
                serializer.serialize(rowData, wrapper);
                boolean duplicated = !buffer.add(baos.toByteArray());
                if (duplicated) {
                    return;
                }

                currentSize += baos.size();
            }

            if (exceedThreshold()) {
                // Clear the filtering data and disable self by leaving the currentSize unchanged
                buffer.clear();
                LOG.warn(
                        "Collected data size exceeds the threshold, {} > {}, dynamic filtering is disabled.",
                        currentSize,
                        rowDataThreshold);
            }
        }

        @Override
        public DynamicFilteringDataRuntimeFilter build() {
            final DynamicFilteringData dynamicFilteringData;
            if (exceedThreshold()) {
                dynamicFilteringData =
                        new DynamicFilteringData(
                                typeInfo, projectedType, Collections.emptyList(), false);
            } else {
                dynamicFilteringData =
                        new DynamicFilteringData(
                                typeInfo, projectedType, new ArrayList<>(buffer), true);
            }
            return new DynamicFilteringDataRuntimeFilter(dynamicFilteringData);
        }

        private boolean exceedThreshold() {
            return rowDataThreshold > 0 && currentSize > rowDataThreshold;
        }
    }

    private static class BloomFilterBuilder implements FilterBuilder {
        private final RowType rowType;
        private final int estimatedBuildCount;
        private final List<Tuple2<Integer, Integer>> joinKeys;
        private final List<FieldGetter> dimKeyGetters;

        // TODO config?
        private int bloomFilterByteSize = 4 * 1024 * 1024;

        private List<SerializableFunction<RowData, Object>> factKeyExtractors;

        private final MemorySegment segment;

        private final BloomFilter filter;

        // joinKeys:fact->dim
        public BloomFilterBuilder(
                RowType rowType, int estimatedBuildCount, List<Tuple2<Integer, Integer>> joinKeys) {
            this.rowType = rowType;
            this.estimatedBuildCount = estimatedBuildCount;
            this.joinKeys = joinKeys;

            this.filter = new BloomFilter(estimatedBuildCount, bloomFilterByteSize);
            this.segment = MemorySegmentFactory.allocateUnpooledSegment(bloomFilterByteSize);
            filter.setBitsLocation(segment, 0);

            this.dimKeyGetters =
                    joinKeys.stream()
                            .map(
                                    joinKey ->
                                            RowData.createFieldGetter(
                                                    rowType.getTypeAt(joinKey.f1), joinKey.f1))
                            .collect(Collectors.toList());
            this.factKeyExtractors =
                    joinKeys.stream()
                            .map(
                                    joinKey ->
                                            RowData.createFieldGetter(
                                                    rowType.getTypeAt(joinKey.f1), joinKey.f0))
                            .map(
                                    fieldGetter ->
                                            (SerializableFunction<RowData, Object>)
                                                    fieldGetter::getFieldOrNull)
                            .collect(Collectors.toList());
        }

        @Override
        public void add(RowData value) {
            filter.addHash(
                    Arrays.hashCode(
                            dimKeyGetters.stream()
                                    .map(getter -> getter.getFieldOrNull(value))
                                    .toArray()));
        }

        @Override
        public BloomRuntimeFilter<RowData> build() {
            return new BloomRuntimeFilter<>(
                    estimatedBuildCount,
                    bloomFilterByteSize,
                    segment.getHeapMemory(),
                    factKeyExtractors);
        }
    }
}
