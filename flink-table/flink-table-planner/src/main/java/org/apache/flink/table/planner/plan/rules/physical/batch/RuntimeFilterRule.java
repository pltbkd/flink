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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.runtime.source.coordinator.RuntimeFilterEvent.ReceiverType;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringDataCollector;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicFilteringTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalHashJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalSortMergeJoin;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.runtime.operators.dynamicfiltering.DynamicFilteringDataCollectorOperator.FilterConfig;
import org.apache.flink.table.runtime.operators.dynamicfiltering.DynamicFilteringDataCollectorOperator.RuntimeFilterType;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Exchange;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.mapping.IntPair;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/** RuntimeFilterRule. */
public class RuntimeFilterRule extends RelRule<RelRule.Config> {
    public RuntimeFilterRule(Config config) {
        super(config);
    }

    public static final RelOptRule INSTANCE = Config.DEFAULT.toRule();
    /** Config. */
    public interface Config extends RelRule.Config {
        @Override
        default RuntimeFilterRule toRule() {
            return new RuntimeFilterRule(this);
        }

        RelRule.Config DEFAULT =
                RelRule.Config.EMPTY
                        .withDescription("RuntimeFilterRule")
                        .withOperandSupplier(
                                o ->
                                        o.operand(BatchPhysicalHashJoin.class)
                                                .predicate(join -> !join.runtimeFilterVisited())
                                                .anyInputs())
                        .as(RuntimeFilterRule.Config.class);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalHashJoin join = call.rel(0);
        join.runtimeFilterVisited_$eq(true);

        boolean leftIsDim = join.leftIsBuild();
        // trust leftIsBuild and take build side as dim side
        RelNode dimRel = leftIsDim ? join.getLeft() : join.getRight();
        RelNode factRel = leftIsDim ? join.getRight() : join.getLeft();

        if (!((dimRel instanceof HepRelVertex)
                && ((HepRelVertex) dimRel).getCurrentRel() instanceof BatchPhysicalExchange)) {
            // TODO unsupported pattern?
            return;
        }

        JoinInfo joinInfo = join.analyzeCondition();
        ImmutableIntList factKeys = leftIsDim ? joinInfo.rightKeys : joinInfo.leftKeys;
        ImmutableIntList dimKeys = leftIsDim ? joinInfo.leftKeys : joinInfo.rightKeys;
        List<IntPair> joinKeys = IntPair.zip(factKeys, dimKeys);

        Map<BatchPhysicalTableSourceScan, List<IntPair>> sourceFilters = new HashMap<>();
        // try push joinKeys
        join.accept(new PushFilterKeysShuttle(joinKeys, sourceFilters));
        if (sourceFilters.isEmpty()) {
            return;
        }

        //        BatchPhysicalRuntimeFilterBuilder filterBuilder = createFilterBuilder(dimRel,
        // dimKeys);

        // create source
        RelNode newFactRel = factRel.accept(new PushFilterInSourceShuttle(sourceFilters, dimRel));
        List<RelNode> newInputs =
                leftIsDim ? Arrays.asList(dimRel, newFactRel) : Arrays.asList(newFactRel, dimRel);
        Join newJoin = join.copy(join.getTraitSet(), newInputs);

        call.transformTo(newJoin);
    }

    //    private static RelNode createNewTableSourceScan(
    //            BatchPhysicalTableSourceScan sourceRel,
    //            BatchPhysicalRuntimeFilterBuilder filterBuilderRel,
    //            List<IntPair> joinKeys) {
    //        TableSourceTable tableSourceTable =
    // sourceRel.getTable().unwrap(TableSourceTable.class);
    //        String uuid = filterBuilderRel.registerFilter(joinKeys);
    //        return sourceRel.copy(tableSourceTable, filterBuilderRel, uuid, joinKeys);
    //    }

    //    private BatchPhysicalRuntimeFilterBuilder createFilterBuilder(
    //            RelNode dimRel, ImmutableIntList dimKeys) {
    //        return new BatchPhysicalRuntimeFilterBuilder(
    //                dimRel.getCluster(),
    //                dimRel.getCluster().traitSetOf(FlinkConventions.BATCH_PHYSICAL()),
    //                // TODO why?
    //                ((HepRelVertex) dimRel).getCurrentRel(),
    //                dimKeys);
    //    }

    //        public static class BatchPhysicalRuntimeFilterBuilder extends SingleRel
    //                implements BatchPhysicalRel {
    //
    //            private final ImmutableIntList dimKeys;
    //            private final Map<String, List<IntPair>> joinKeysRegistration;
    //
    //            protected BatchPhysicalRuntimeFilterBuilder(
    //                    RelOptCluster cluster,
    //                    RelTraitSet traits,
    //                    RelNode input,
    //                    ImmutableIntList dimKeys) {
    //                this(cluster, traits, input, dimKeys, new HashMap<>());
    //            }
    //
    //            private BatchPhysicalRuntimeFilterBuilder(
    //                    RelOptCluster cluster,
    //                    RelTraitSet traits,
    //                    RelNode input,
    //                    ImmutableIntList dimKeys,
    //                    Map<String, List<IntPair>> joinKeysRegistration) {
    //                super(cluster, traits, input);
    //                this.dimKeys = dimKeys;
    //                this.joinKeysRegistration = joinKeysRegistration;
    //            }
    //
    //            public BatchPhysicalRuntimeFilterBuilder copy(
    //                    BatchPhysicalRuntimeFilterBuilder filter, RelNode input) {
    //                return new BatchPhysicalRuntimeFilterBuilder(
    //                        filter.getCluster(),
    //                        filter.getTraitSet(),
    //                        input,
    //                        dimKeys,
    //                        joinKeysRegistration);
    //            }
    //
    //            public String registerFilter(List<IntPair> joinKeys) {
    //                String uuid = UUID.randomUUID().toString();
    //                joinKeysRegistration.put(uuid, joinKeys);
    //                return uuid;
    //            }
    //
    //            @Override
    //            public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    //                return new BatchPhysicalRuntimeFilterBuilder(
    //                        getCluster(), traitSet, input, dimKeys, joinKeysRegistration);
    //            }
    //
    //            @Override
    //            public ExecNode<?> translateToExecNode() {
    //                RelMetadataQuery mq = getCluster().getMetadataQuery();
    //                int dimRowCount = (int) Util.first(mq.getRowCount(getInput()), 200000);
    //                return new BatchExecRuntimeFilterBuilder(
    //                        unwrapTableConfig(this),
    //                        FlinkTypeFactory.toLogicalRowType(input.getRowType()),
    //                        "RuntimeFilterBuilder",
    //                        dimKeys,
    //                        dimRowCount,
    //                        joinKeysRegistration);
    //            }
    //
    //            @Override
    //            public RelWriter explainTerms(RelWriter pw) {
    //                String fields =
    //                        joinKeysRegistration.values().stream()
    //                                .map(
    //                                        keysList ->
    //                                                "["
    //                                                        + keysList.stream()
    //                                                                .map(
    //                                                                        keys ->
    //                                                                                getRowType()
    //
    //     .getFieldNames()
    //                                                                                        .get(
    //
    //     keys.target))
    //
    // .collect(Collectors.joining(","))
    //                                                        + "]")
    //                                .collect(Collectors.joining(", "));
    //                return super.explainTerms(pw).item("fields", fields);
    //            }
    //        }

    //        public static class BatchExecRuntimeFilterBuilder extends ExecNodeBase<Object>
    //                implements BatchExecNode<Object> {
    //
    //            private final ImmutableIntList dimKeys;
    //            private final int estimatedBuildCount;
    //            private final Map<String, List<IntPair>> joinKeysRegistration;
    //
    //            public BatchExecRuntimeFilterBuilder(
    //                    ReadableConfig tableConfig,
    //                    RowType outputType,
    //                    String description,
    //                    ImmutableIntList dimKeys,
    //                    int estimatedBuildCount,
    //                    Map<String, List<IntPair>> joinKeysRegistration) {
    //                super(
    //                        ExecNodeContext.newNodeId(),
    //                        ExecNodeContext.newContext(BatchExecTableSourceScan.class),
    //                        ExecNodeContext.newPersistedConfig(BatchExecTableSourceScan.class,
    //     tableConfig),
    //                        Collections.singletonList(InputProperty.DEFAULT),
    //                        outputType,
    //                        description);
    //                this.dimKeys = dimKeys;
    //                this.estimatedBuildCount = estimatedBuildCount;
    //                this.joinKeysRegistration = joinKeysRegistration;
    //            }
    //
    //            @Override
    //            @SuppressWarnings("unchecked")
    //            protected Transformation<Object> translateToPlanInternal(
    //                    PlannerBase planner, ExecNodeConfig config) {
    //                final ExecEdge inputEdge = getInputEdges().get(0);
    //                final Transformation<RowData> inputTransform =
    //                        (Transformation<RowData>) inputEdge.translateToPlan(planner);
    //                StreamOperatorFactory<Object> factory =
    //                        new RuntimeFilterOperatorFactory(
    //                                (RowType) getOutputType(), estimatedBuildCount,
    //     joinKeysRegistration);
    //                factory.setChainingStrategy(ChainingStrategy.ALWAYS);
    //
    //                return ExecNodeUtil.createOneInputTransformation(
    //                        inputTransform,
    //                        createTransformationName(config),
    //                        createTransformationDescription(config),
    //                        factory,
    //                        InternalTypeInfo.of(getOutputType()),
    //                        1); // parallelism should always be 1
    //            }
    //        }

    //        public static class RuntimeFilterOperatorFactory extends
    //     AbstractStreamOperatorFactory<Object>
    //                implements CoordinatedOperatorFactory<Object> {
    //
    //            private final RowType rowType;
    //            private final int estimatedBuildCount;
    //            private final Map<String, List<Tuple2<Integer, Integer>>> joinKeysRegistration;
    //
    //            public RuntimeFilterOperatorFactory(
    //                    RowType rowType,
    //                    int estimatedBuildCount,
    //                    Map<String, List<IntPair>> joinKeysRegistration) {
    //                this.rowType = rowType;
    //                this.estimatedBuildCount = estimatedBuildCount;
    //                this.joinKeysRegistration = new HashMap<>();
    //                joinKeysRegistration.forEach(
    //                        (key, value) ->
    //                                this.joinKeysRegistration.put(
    //                                        key,
    //                                        value.stream()
    //                                                .map(p -> new Tuple2<>(p.source, p.target))
    //                                                .collect(Collectors.toList())));
    //            }
    //
    //            @Override
    //            public <T extends StreamOperator<Object>> T createStreamOperator(
    //                    StreamOperatorParameters<Object> parameters) {
    //                final OperatorID operatorId = parameters.getStreamConfig().getOperatorID();
    //                final OperatorEventDispatcher eventDispatcher =
    //     parameters.getOperatorEventDispatcher();
    //                OperatorEventGateway operatorEventGateway =
    //                        eventDispatcher.getOperatorEventGateway(operatorId);
    //
    //                RuntimeFilterOperator operator =
    //                        new RuntimeFilterOperator(
    //                                rowType,
    //                                estimatedBuildCount,
    //                                joinKeysRegistration,
    //                                operatorEventGateway);
    //                operator.setup(
    //                        parameters.getContainingTask(),
    //                        parameters.getStreamConfig(),
    //                        parameters.getOutput());
    //
    //                // today's lunch is generics spaghetti
    //                @SuppressWarnings("unchecked")
    //                final T castedOperator = (T) operator;
    //
    //                return castedOperator;
    //            }
    //
    //            @SuppressWarnings("rawtypes")
    //            @Override
    //            public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader
    // classLoader)
    //     {
    //                return RuntimeFilterOperator.class;
    //            }
    //
    //            @Override
    //            public OperatorCoordinator.Provider getCoordinatorProvider(
    //                    String operatorName, OperatorID operatorID) {
    //                return new RuntimeFilterOperatorCoordinator.Provider(operatorID);
    //            }
    //        }

    //        public static class RuntimeFilterOperator extends AbstractStreamOperator<Object>
    //                implements OneInputStreamOperator<RowData, Object> {
    //            private final RowType rowType;
    //            private final int estimatedBuildCount;
    //            private final Map<String, List<Tuple2<Integer, Integer>>> joinKeysRegistration;
    //            private final OperatorEventGateway operatorEventGateway;
    //
    //            private final Map<String, BloomFilter> bloomFilterHolder;
    //            private final Map<String, MemorySegment> bloomFilterMemorySegmentHolder;
    //
    //            private int bloomFilterByteSize = 64 * 1024 * 1024;
    //            private List<SerializableFunction<RowData, Object>> keyExtractors = 1;
    //
    //            public RuntimeFilterOperator(
    //                    RowType rowType,
    //                    int estimatedBuildCount,
    //                    Map<String, List<Tuple2<Integer, Integer>>> joinKeysRegistration,
    //                    OperatorEventGateway operatorEventGateway) {
    //                this.rowType = rowType;
    //                this.estimatedBuildCount = estimatedBuildCount;
    //                this.joinKeysRegistration = joinKeysRegistration;
    //                this.operatorEventGateway = operatorEventGateway;
    //                this.bloomFilterHolder = new HashMap<>();
    //                this.bloomFilterMemorySegmentHolder = new HashMap<>();
    //            }
    //
    //            @Override
    //            public void processElement(StreamRecord<RowData> element) throws Exception {
    //                for (Map.Entry<String, List<Tuple2<Integer, Integer>>> joinKeysEntry :
    //                        joinKeysRegistration.entrySet()) {
    //                    BloomFilter filter =
    //                            bloomFilterHolder.computeIfAbsent(
    //                                    joinKeysEntry.getKey(),
    //                                    k -> {
    //                                        BloomFilter newFilter =
    //                                                new BloomFilter(
    //                                                        estimatedBuildCount,
    // bloomFilterByteSize);
    //                                        MemorySegment segment =
    //                                                MemorySegmentFactory.allocateUnpooledSegment(
    //                                                        bloomFilterByteSize);
    //                                        bloomFilterMemorySegmentHolder.put(k, segment);
    //                                        newFilter.setBitsLocation(segment, 0);
    //                                        return newFilter;
    //                                    });
    //                    Object[] values = new Object[joinKeysEntry.getValue().size()];
    //                    for (int i = 0; i < values.length; i++) {
    //                        int index = joinKeysEntry.getValue().get(i).f1;
    //                        LogicalType type = rowType.getTypeAt(index);
    //                        switch (type.getTypeRoot()) {
    //                            case INTEGER:
    //                                values[i] = element.getValue().getInt(index);
    //                                break;
    //                            case BIGINT:
    //                                values[i] = element.getValue().getLong(index);
    //                                break;
    //                            case VARCHAR:
    //                                values[i] = element.getValue().getString(index).toString();
    //                                break;
    //                            default:
    //                                // TODO more types?
    //                                throw new UnsupportedOperationException();
    //                        }
    //                    }
    //                    filter.addHash(Arrays.hashCode(values));
    //                }
    //            }
    //
    //            public void finish() throws Exception {
    //                for (Map.Entry<String, MemorySegment> e :
    //     bloomFilterMemorySegmentHolder.entrySet()) {
    //                    MemorySegment memorySegment = e.getValue();
    //                    RuntimeFilter<RowData> filter =
    //                            new BloomRuntimeFilter<>(
    //                                    estimatedBuildCount,
    //                                    bloomFilterByteSize,
    //                                    memorySegment.getHeapMemory(),
    //                                    keyExtractors);
    //                    RuntimeFilterEvent<RowData> event = new RuntimeFilterEvent<>(e.getKey(),
    //     filter);
    //                    operatorEventGateway.sendEventToCoordinator(new
    // SourceEventWrapper(event));
    //                }
    //            }
    //        }

    //        public static class RuntimeFilterOperatorCoordinator
    //                implements OperatorCoordinator, CoordinationRequestHandler {
    //
    //            private final CoordinatorStore coordinatorStore;
    //
    //            public RuntimeFilterOperatorCoordinator(Context context) {
    //                this.coordinatorStore = context.getCoordinatorStore();
    //            }
    //
    //            @Override
    //            public void start() throws Exception {}
    //
    //            @Override
    //            public void close() throws Exception {}
    //
    //            @Override
    //            public void handleEventFromOperator(int subtask, int attempt, OperatorEvent event)
    //                    throws Exception {
    //                RuntimeFilterEvent filterEvent =
    //                        (RuntimeFilterEvent) ((SourceEventWrapper) event).getSourceEvent();
    //                // push event
    //                OperatorCoordinator listener =
    //                        (OperatorCoordinator)
    // coordinatorStore.get(filterEvent.getReceiverUUID());
    //                if (listener == null) {
    //                    throw new IllegalStateException("Partition data listener missing");
    //                }
    //                listener.handleEventFromOperator(0, 0, new SourceEventWrapper(filterEvent));
    //            }
    //
    //            @Override
    //            public CompletableFuture<CoordinationResponse> handleCoordinationRequest(
    //                    CoordinationRequest request) {
    //                throw new UnsupportedOperationException();
    //            }
    //
    //            @Override
    //            public void subtaskAttemptFailed(int subtask, int attempt, @Nullable Throwable
    // reason)
    //     {
    //                // subtask failed, the socket server does not exist anymore
    //            }
    //
    //            @Override
    //            public void subtaskReset(int subtask, long checkpointId) {
    //                // nothing to do here, connections are re-created lazily
    //            }
    //
    //            @Override
    //            public void subtaskAttemptReady(int subtask, int attempt, SubtaskGateway gateway)
    // {
    //                // nothing to do here, connections are re-created lazily
    //            }
    //
    //            @Override
    //            public void checkpointCoordinator(long checkpointId, CompletableFuture<byte[]>
    // result)
    //                    throws Exception {}
    //
    //            @Override
    //            public void notifyCheckpointComplete(long checkpointId) {}
    //
    //            @Override
    //            public void resetToCheckpoint(long checkpointId, @Nullable byte[] checkpointData)
    //                    throws Exception {}
    //
    //            public static class Provider implements OperatorCoordinator.Provider {
    //
    //                private final OperatorID operatorID;
    //
    //                public Provider(OperatorID operatorID) {
    //                    this.operatorID = operatorID;
    //                }
    //
    //                @Override
    //                public OperatorID getOperatorId() {
    //                    return operatorID;
    //                }
    //
    //                @Override
    //                public OperatorCoordinator create(Context context) {
    //                    return new RuntimeFilterOperatorCoordinator(context);
    //                }
    //            }
    //        }

    private static class PushFilterInSourceShuttle extends DefaultRelShuttle {
        //        private final BatchPhysicalRuntimeFilterBuilder filterBuilder;
        private final Map<BatchPhysicalTableSourceScan, List<IntPair>> sourceFilters;
        private final RelNode dimRel;

        private PushFilterInSourceShuttle(
                Map<BatchPhysicalTableSourceScan, List<IntPair>> sourceFilters, RelNode dimRel) {
            //            this.filterBuilder = filterBuilder;
            this.sourceFilters = sourceFilters;
            this.dimRel = ignoreExchange(dimRel);
        }

        @Override
        public RelNode visit(RelNode rel) {
            if (rel instanceof HepRelVertex) {
                // TODO ?should not modify while visiting?
                // throw new RuntimeException();
                HepRelVertex relVertex = (HepRelVertex) rel;
                RelNode newRel = visit(((HepRelVertex) rel).getCurrentRel());
                try {
                    Method replaceRel =
                            HepRelVertex.class.getDeclaredMethod("replaceRel", RelNode.class);
                    replaceRel.setAccessible(true);
                    replaceRel.invoke(relVertex, newRel);
                } catch (NoSuchMethodException
                        | IllegalAccessException
                        | InvocationTargetException e) {
                    throw new RuntimeException(e);
                }
                return relVertex;
            } else if (rel instanceof BatchPhysicalTableSourceScan
                    && sourceFilters.containsKey(rel)) {
                if (rel instanceof BatchPhysicalDynamicFilteringTableSourceScan) {
                    FilterConfig filter =
                            new FilterConfig(
                                    ((BatchPhysicalDynamicFilteringTableSourceScan) rel).uuid(),
                                    sourceFilters.get(rel).stream()
                                            .mapToInt(pair -> pair.target)
                                            .toArray(),
                                    sourceFilters.get(rel).stream()
                                            .mapToInt(pair -> pair.source)
                                            .toArray(),
                                    RuntimeFilterType.BLOOM,
                                    ReceiverType.OPERATOR);
                    for (RelNode input : rel.getInputs()) {
                        input = ignoreExchange(input);
                        if (!(input instanceof BatchPhysicalDynamicFilteringDataCollector)
                                || input.getInputs().size() != 1) {
                            // TODO ?
                            throw new RuntimeException();
                        }
                        RelNode dimInput =
                                ignoreExchange(
                                        ((BatchPhysicalDynamicFilteringDataCollector) input)
                                                .getInput());
                        dimInput =
                                dimInput instanceof HepRelVertex
                                        ? ((HepRelVertex) dimInput).getCurrentRel()
                                        : dimInput;
                        if (dimInput == dimRel) {
                            ((BatchPhysicalDynamicFilteringDataCollector) input)
                                    .filterConfigs()
                                    .add(filter);
                            return rel;
                        }
                    }
                    // No collector for current dim, create new one
                    BatchPhysicalDynamicFilteringDataCollector newCollector =
                            new BatchPhysicalDynamicFilteringDataCollector(
                                    dimRel.getCluster(),
                                    dimRel.getTraitSet(),
                                    ignoreExchange(dimRel),
                                    dimRel.getRowType(),
                                    new ArrayList<>());
                    newCollector.filterConfigs().add(filter);
                    ArrayList<RelNode> newInputs = new ArrayList<>(rel.getInputs());
                    newInputs.add(newCollector);
                    return rel.copy(rel.getTraitSet(), newInputs);
                } else {
                    // No dpp, build new rf source
                    BatchPhysicalDynamicFilteringDataCollector dynamicFilteringDataCollector =
                            new BatchPhysicalDynamicFilteringDataCollector(
                                    dimRel.getCluster(),
                                    dimRel.getTraitSet(),
                                    ignoreExchange(dimRel),
                                    dimRel.getRowType(),
                                    new ArrayList<>());
                    BatchPhysicalDynamicFilteringTableSourceScan newScan =
                            new BatchPhysicalDynamicFilteringTableSourceScan(
                                    rel.getCluster(),
                                    rel.getTraitSet(),
                                    ((BatchPhysicalTableSourceScan) rel).getHints(),
                                    ((BatchPhysicalTableSourceScan) rel).tableSourceTable(),
                                    Collections.singletonList(dynamicFilteringDataCollector));
                    FilterConfig filter =
                            new FilterConfig(
                                    newScan.uuid(),
                                    sourceFilters.get(rel).stream()
                                            .mapToInt(pair -> pair.target)
                                            .toArray(),
                                    sourceFilters.get(rel).stream()
                                            .mapToInt(pair -> pair.source)
                                            .toArray(),
                                    RuntimeFilterType.BLOOM,
                                    ReceiverType.OPERATOR);
                    dynamicFilteringDataCollector.filterConfigs().add(filter);
                    return newScan;
                }
            } else {
                return super.visit(rel);
            }
        }
    }

    private static RelNode ignoreExchange(RelNode dimSide) {
        if (dimSide instanceof HepRelVertex) {
            dimSide = ((HepRelVertex) dimSide).getCurrentRel();
        }
        if (dimSide instanceof Exchange) {
            RelNode input = dimSide.getInput(0);
            if (input instanceof HepRelVertex) {
                input = ((HepRelVertex) input).getCurrentRel();
            }
            return input;
        } else {
            return dimSide;
        }
    }

    public static class PushFilterKeysShuttle extends RelHomogeneousShuttle {
        private final List<IntPair> keys;
        private final Map<BatchPhysicalTableSourceScan, List<IntPair>> sourceFilters;

        public PushFilterKeysShuttle(
                List<IntPair> keys,
                Map<BatchPhysicalTableSourceScan, List<IntPair>> sourceFilters) {
            this.keys = keys;
            this.sourceFilters = sourceFilters;
        }

        @Override
        // return value is not important
        public RelNode visit(RelNode rel) {
            if (rel instanceof HepRelVertex) {
                return visit(((HepRelVertex) rel).getCurrentRel());
            } else if (rel instanceof Exchange || rel instanceof Filter) {
                return visitChildren(rel);
            } else if (rel instanceof Join) {
                Join join = (Join) rel;
                int leftFields = join.getLeft().getRowType().getFieldCount();
                int rightFields = join.getRight().getRowType().getFieldCount();
                Map<Integer, Integer> leftProjectedToOriginal =
                        IntStream.range(0, leftFields)
                                .boxed()
                                .collect(Collectors.toMap(i -> i, i -> i));
                Map<Integer, Integer> rightProjectedToOriginal =
                        IntStream.range(leftFields, leftFields + rightFields)
                                .boxed()
                                .collect(Collectors.toMap(i -> i, i -> i - leftFields));
                if (join instanceof BatchPhysicalHashJoin) {
                    // Push only fact side, trust buildIsLeft
                    Map<Integer, Integer> factProjectedToOriginal =
                            ((BatchPhysicalHashJoin) join).leftIsBuild()
                                    ? rightProjectedToOriginal
                                    : leftProjectedToOriginal;
                    List<IntPair> originalKeys = calcOriginalKeys(keys, factProjectedToOriginal);
                    System.out.println(
                            "projected keys:"
                                    + keys.stream()
                                            .map(
                                                    p ->
                                                            rel.getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(","))
                                    + ", original keys:"
                                    + originalKeys.stream()
                                            .map(
                                                    p ->
                                                            (((BatchPhysicalHashJoin) join)
                                                                                    .leftIsBuild()
                                                                            ? join.getRight()
                                                                            : join.getLeft())
                                                                    .getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(",")));
                    if (!originalKeys.isEmpty()) {
                        new PushFilterKeysShuttle(originalKeys, sourceFilters)
                                .visit(
                                        ((BatchPhysicalHashJoin) join).leftIsBuild()
                                                ? join.getRight()
                                                : join.getLeft());
                    }
                } else if (join instanceof BatchPhysicalSortMergeJoin) {
                    // Push both
                    List<IntPair> leftOriginalKeys =
                            calcOriginalKeys(keys, leftProjectedToOriginal);
                    System.out.println(
                            "projected keys:"
                                    + keys.stream()
                                            .map(
                                                    p ->
                                                            rel.getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(","))
                                    + ", original keys:"
                                    + leftOriginalKeys.stream()
                                            .map(
                                                    p ->
                                                            join.getLeft()
                                                                    .getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(",")));
                    if (!leftOriginalKeys.isEmpty()) {
                        new PushFilterKeysShuttle(leftOriginalKeys, sourceFilters)
                                .visit(join.getLeft());
                    }
                    List<IntPair> rightOriginalKeys =
                            calcOriginalKeys(keys, rightProjectedToOriginal);
                    System.out.println(
                            "projected keys:"
                                    + keys.stream()
                                            .map(
                                                    p ->
                                                            rel.getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(","))
                                    + ", original keys:"
                                    + rightOriginalKeys.stream()
                                            .map(
                                                    p ->
                                                            join.getRight()
                                                                    .getRowType()
                                                                    .getFieldNames()
                                                                    .get(p.source))
                                            .collect(Collectors.joining(",")));
                    if (!rightOriginalKeys.isEmpty()) {
                        new PushFilterKeysShuttle(rightOriginalKeys, sourceFilters)
                                .visit(join.getRight());
                    }
                }
                // else not supported yet, do not push
                return null;
            } else if (rel instanceof Project || rel instanceof Calc) {
                List<RexNode> projects;
                if (rel instanceof Project) {
                    projects = ((Project) rel).getProjects();
                } else {
                    projects =
                            ((Calc) rel)
                                    .getProgram().getProjectList().stream()
                                            .map(p -> ((Calc) rel).getProgram().expandLocalRef(p))
                                            .collect(Collectors.toList());
                }
                Map<Integer, Integer> projectedToOriginal = new HashMap<>();
                for (int i = 0; i < projects.size(); i++) {
                    RexNode rex = projects.get(i);
                    if (rex instanceof RexInputRef) {
                        projectedToOriginal.put(i, ((RexInputRef) rex).getIndex());
                    }
                }
                List<IntPair> originalKeys = calcOriginalKeys(keys, projectedToOriginal);
                System.out.println(
                        "projected keys:"
                                + keys.stream()
                                        .map(p -> rel.getRowType().getFieldNames().get(p.source))
                                        .collect(Collectors.joining(","))
                                + ", original keys:"
                                + originalKeys.stream()
                                        .map(
                                                p ->
                                                        rel.getInput(0)
                                                                .getRowType()
                                                                .getFieldNames()
                                                                .get(p.source))
                                        .collect(Collectors.joining(",")));
                if (originalKeys.isEmpty()) {
                    // Keys are not projected but calculated. Can not push any further.
                    return null;
                }
                return new PushFilterKeysShuttle(originalKeys, sourceFilters).visitChildren(rel);
            } else if (rel instanceof BatchPhysicalTableSourceScan) {
                sourceFilters.put((BatchPhysicalTableSourceScan) rel, keys);
                return null;
            } else {
                // Do not push any further.
                return null;
            }
        }

        private List<IntPair> calcOriginalKeys(
                List<IntPair> keys, Map<Integer, Integer> projectedToOriginal) {
            return keys.stream()
                    .filter(pair -> projectedToOriginal.containsKey(pair.source))
                    .map(pair -> new IntPair(projectedToOriginal.get(pair.source), pair.target))
                    .collect(Collectors.toList());
        }
    }
}
