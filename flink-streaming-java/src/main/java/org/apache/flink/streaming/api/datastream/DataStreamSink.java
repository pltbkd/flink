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

package org.apache.flink.streaming.api.datastream;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.Public;
import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.operators.ResourceSpec;
import org.apache.flink.api.common.operators.SlotSharingGroup;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableMessageTypeInfo;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.TwoPhaseCommittingSink;
import org.apache.flink.api.connector.sink2.WithPostCommitTopology;
import org.apache.flink.api.connector.sink2.WithPreCommitTopology;
import org.apache.flink.api.connector.sink2.WithPreWriteTopology;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamSink;
import org.apache.flink.streaming.api.transformations.LegacySinkTransformation;
import org.apache.flink.streaming.api.transformations.PartitionTransformation;
import org.apache.flink.streaming.api.transformations.PhysicalTransformation;
import org.apache.flink.streaming.api.transformations.SinkTransformation;
import org.apache.flink.streaming.api.transformations.SinkV1Adapter;
import org.apache.flink.streaming.api.transformations.StreamExchangeMode;
import org.apache.flink.streaming.runtime.operators.sink.CommitterOperatorFactory;
import org.apache.flink.streaming.runtime.operators.sink.WriterOperatorFactory;
import org.apache.flink.streaming.runtime.partitioner.ForwardPartitioner;

import java.util.List;
import java.util.Optional;
import java.util.function.BiConsumer;
import java.util.function.Function;

/**
 * A Stream Sink. This is used for emitting elements from a streaming topology.
 *
 * @param <T> The type of the elements in the Stream
 */
@Public
public class DataStreamSink<T> {

    private final PhysicalTransformation<T> transformation;

    protected DataStreamSink(PhysicalTransformation<T> transformation) {
        this.transformation = transformation;
    }

    @SuppressWarnings("unchecked")
    static <T> DataStreamSink<T> forSinkFunction(
            DataStream<T> inputStream, SinkFunction<T> sinkFunction) {
        StreamSink<T> sinkOperator = new StreamSink<>(sinkFunction);
        PhysicalTransformation<T> transformation =
                new LegacySinkTransformation<>(
                        inputStream.getTransformation(),
                        "Unnamed",
                        sinkOperator,
                        inputStream.getExecutionEnvironment().getParallelism());
        inputStream.getExecutionEnvironment().addOperator(transformation);
        return new DataStreamSink<>(transformation);
    }

    @SuppressWarnings("unchecked")
    static <T> DataStreamSink<T> forSink(DataStream<T> inputStream, Sink<T> sink) {
        StreamExecutionEnvironment executionEnvironment = inputStream.getExecutionEnvironment();
        SinkTransformation<T, T> transformation =
                new SinkTransformation<>(
                        inputStream.getTransformation(),
                        inputStream.getType(),
                        "Sink",
                        executionEnvironment.getParallelism());
        DataStreamSink<T> dataStreamSink = new DataStreamSink<>(transformation);
        SinkExpander<T> sinkExpander = new SinkExpander<>(inputStream, sink, transformation);
        executionEnvironment.registerTranslationListener(sinkExpander::expand);
        return dataStreamSink;
    }

    public static <T> DataStreamSink<T> forSinkV1(
            DataStream<T> inputStream, org.apache.flink.api.connector.sink.Sink<T, ?, ?, ?> sink) {
        return forSink(inputStream, SinkV1Adapter.wrap(sink));
    }

    /** Returns the transformation that contains the actual sink operator of this sink. */
    @Internal
    public Transformation<?> getTransformation() {
        return transformation;
    }

    @Internal
    public LegacySinkTransformation<T> getLegacyTransformation() {
        if (transformation instanceof LegacySinkTransformation) {
            return (LegacySinkTransformation<T>) transformation;
        } else {
            throw new IllegalStateException("There is no the LegacySinkTransformation.");
        }
    }

    /**
     * Sets the name of this sink. This name is used by the visualization and logging during
     * runtime.
     *
     * @return The named sink.
     */
    public DataStreamSink<T> name(String name) {
        transformation.setName(name);
        return this;
    }

    /**
     * Sets an ID for this operator.
     *
     * <p>The specified ID is used to assign the same operator ID across job submissions (for
     * example when starting a job from a savepoint).
     *
     * <p><strong>Important</strong>: this ID needs to be unique per transformation and job.
     * Otherwise, job submission will fail.
     *
     * @param uid The unique user-specified ID of this transformation.
     * @return The operator with the specified ID.
     */
    @PublicEvolving
    public DataStreamSink<T> uid(String uid) {
        transformation.setUid(uid);
        return this;
    }

    /**
     * Sets an user provided hash for this operator. This will be used AS IS the create the
     * JobVertexID.
     *
     * <p>The user provided hash is an alternative to the generated hashes, that is considered when
     * identifying an operator through the default hash mechanics fails (e.g. because of changes
     * between Flink versions).
     *
     * <p><strong>Important</strong>: this should be used as a workaround or for trouble shooting.
     * The provided hash needs to be unique per transformation and job. Otherwise, job submission
     * will fail. Furthermore, you cannot assign user-specified hash to intermediate nodes in an
     * operator chain and trying so will let your job fail.
     *
     * <p>A use case for this is in migration between Flink versions or changing the jobs in a way
     * that changes the automatically generated hashes. In this case, providing the previous hashes
     * directly through this method (e.g. obtained from old logs) can help to reestablish a lost
     * mapping from states to their target operator.
     *
     * @param uidHash The user provided hash for this operator. This will become the JobVertexID,
     *     which is shown in the logs and web ui.
     * @return The operator with the user provided hash.
     */
    @PublicEvolving
    public DataStreamSink<T> setUidHash(String uidHash) {
        if (!(transformation instanceof LegacySinkTransformation)) {
            throw new UnsupportedOperationException(
                    "Cannot set a custom UID hash on a non-legacy sink");
        }
        transformation.setUidHash(uidHash);
        return this;
    }

    /**
     * Sets the parallelism for this sink. The degree must be higher than zero.
     *
     * @param parallelism The parallelism for this sink.
     * @return The sink with set parallelism.
     */
    public DataStreamSink<T> setParallelism(int parallelism) {
        transformation.setParallelism(parallelism);
        return this;
    }

    /**
     * Sets the description for this sink.
     *
     * <p>Description is used in json plan and web ui, but not in logging and metrics where only
     * name is available. Description is expected to provide detailed information about the sink,
     * while name is expected to be more simple, providing summary information only, so that we can
     * have more user-friendly logging messages and metric tags without losing useful messages for
     * debugging.
     *
     * @param description The description for this sink.
     * @return The sink with new description.
     */
    @PublicEvolving
    public DataStreamSink<T> setDescription(String description) {
        transformation.setDescription(description);
        return this;
    }

    //	---------------------------------------------------------------------------
    //	 Fine-grained resource profiles are an incomplete work-in-progress feature
    //	 The setters are hence private at this point.
    //	---------------------------------------------------------------------------

    /**
     * Sets the minimum and preferred resources for this sink, and the lower and upper resource
     * limits will be considered in resource resize feature for future plan.
     *
     * @param minResources The minimum resources for this sink.
     * @param preferredResources The preferred resources for this sink
     * @return The sink with set minimum and preferred resources.
     */
    private DataStreamSink<T> setResources(
            ResourceSpec minResources, ResourceSpec preferredResources) {
        transformation.setResources(minResources, preferredResources);

        return this;
    }

    /**
     * Sets the resources for this sink, the minimum and preferred resources are the same by
     * default.
     *
     * @param resources The resources for this sink.
     * @return The sink with set minimum and preferred resources.
     */
    private DataStreamSink<T> setResources(ResourceSpec resources) {
        transformation.setResources(resources, resources);

        return this;
    }

    /**
     * Turns off chaining for this operator so thread co-location will not be used as an
     * optimization.
     *
     * <p>Chaining can be turned off for the whole job by {@link
     * org.apache.flink.streaming.api.environment.StreamExecutionEnvironment#disableOperatorChaining()}
     * however it is not advised for performance considerations.
     *
     * @return The sink with chaining disabled
     */
    @PublicEvolving
    public DataStreamSink<T> disableChaining() {
        this.transformation.setChainingStrategy(ChainingStrategy.NEVER);
        return this;
    }

    /**
     * Sets the slot sharing group of this operation. Parallel instances of operations that are in
     * the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Operations inherit the slot sharing group of input operations if all input operations are
     * in the same slot sharing group and no slot sharing group was explicitly specified.
     *
     * <p>Initially an operation is in the default slot sharing group. An operation can be put into
     * the default group explicitly by setting the slot sharing group to {@code "default"}.
     *
     * @param slotSharingGroup The slot sharing group name.
     */
    @PublicEvolving
    public DataStreamSink<T> slotSharingGroup(String slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    /**
     * Sets the slot sharing group of this operation. Parallel instances of operations that are in
     * the same slot sharing group will be co-located in the same TaskManager slot, if possible.
     *
     * <p>Operations inherit the slot sharing group of input operations if all input operations are
     * in the same slot sharing group and no slot sharing group was explicitly specified.
     *
     * <p>Initially an operation is in the default slot sharing group. An operation can be put into
     * the default group explicitly by setting the slot sharing group with name {@code "default"}.
     *
     * @param slotSharingGroup which contains name and its resource spec.
     */
    @PublicEvolving
    public DataStreamSink<T> slotSharingGroup(SlotSharingGroup slotSharingGroup) {
        transformation.setSlotSharingGroup(slotSharingGroup);
        return this;
    }

    /**
     * Expands the FLIP-143 Sink to a subtopology. Each part of the topology is created after the
     * previous part of the topology has been completely configured by the user. For example, if a
     * user explicitly sets the parallelism of the sink, each part of the subtopology can rely on
     * the input having that parallelism.
     */
    private static class SinkExpander<T> {
        private final SinkTransformation<T, T> transformation;
        private final Sink<T> sink;
        private final DataStream<T> inputStream;
        private final StreamExecutionEnvironment executionEnvironment;
        private boolean expanded;

        public SinkExpander(
                DataStream<T> inputStream, Sink<T> sink, SinkTransformation<T, T> transformation) {
            this.inputStream = inputStream;
            this.executionEnvironment = inputStream.getExecutionEnvironment();
            this.transformation = transformation;
            this.sink = sink;
        }

        private void expand() {
            if (expanded) {
                // may be called twice for multi-staged application, make sure to expand only once
                return;
            }
            expanded = true;

            DataStream<T> prewritten = inputStream;
            if (sink instanceof WithPreWriteTopology) {
                prewritten =
                        adjustTransformations(
                                prewritten, ((WithPreWriteTopology<T>) sink)::addPreWriteTopology);
            }

            if (sink instanceof TwoPhaseCommittingSink) {
                addCommittingTopology(sink, prewritten);
            } else {
                adjustTransformations(
                        prewritten,
                        input ->
                                input.transform(
                                        "Writer",
                                        CommittableMessageTypeInfo.noOutput(),
                                        new WriterOperatorFactory<>(sink)));
            }
        }

        private <T, CommT> void addCommittingTopology(Sink<T> sink, DataStream<T> inputStream) {
            TwoPhaseCommittingSink<T, CommT> committingSink =
                    (TwoPhaseCommittingSink<T, CommT>) sink;
            TypeInformation<CommittableMessage<CommT>> typeInformation =
                    CommittableMessageTypeInfo.forCommittableSerializerFactory(
                            committingSink::getCommittableSerializer);

            DataStream<CommittableMessage<CommT>> written =
                    adjustTransformations(
                            inputStream,
                            input ->
                                    input.transform(
                                            "Writer",
                                            typeInformation,
                                            new WriterOperatorFactory<>(sink)));

            DataStream<CommittableMessage<CommT>> precommitted = addFailOverRegion(written);

            if (sink instanceof WithPreCommitTopology) {
                precommitted =
                        adjustTransformations(
                                precommitted,
                                ((WithPreCommitTopology<T, CommT>) sink)::addPreCommitTopology);
            }
            DataStream<CommittableMessage<CommT>> committed =
                    adjustTransformations(
                            precommitted,
                            pc ->
                                    pc.transform(
                                            "Committer",
                                            typeInformation,
                                            new CommitterOperatorFactory<>(committingSink)));
            if (sink instanceof WithPostCommitTopology) {
                DataStream<CommittableMessage<CommT>> postcommitted = addFailOverRegion(committed);
                adjustTransformations(
                        postcommitted,
                        pc -> {
                            ((WithPostCommitTopology<T, CommT>) sink).addPostCommitTopology(pc);
                            return null;
                        });
            }
        }

        /**
         * Adds a batch exchange that materializes the output first. This is a no-op in STREAMING.
         */
        private <T> DataStream<T> addFailOverRegion(DataStream<T> input) {
            return new DataStream<>(
                    executionEnvironment,
                    new PartitionTransformation<>(
                            input.getTransformation(),
                            new ForwardPartitioner<>(),
                            StreamExchangeMode.BATCH));
        }

        private <T, R> R adjustTransformations(
                DataStream<T> inputStream, Function<DataStream<T>, R> action) {
            int numTransformsBefore = executionEnvironment.getTransformations().size();
            R result = action.apply(inputStream);
            List<Transformation<?>> transformations = executionEnvironment.getTransformations();
            List<Transformation<?>> expandedTransformations =
                    transformations.subList(numTransformsBefore, transformations.size());
            for (Transformation<?> subTransformation : expandedTransformations) {
                concatProperty(subTransformation, Transformation::getName, Transformation::setName);
                concatProperty(
                        subTransformation,
                        Transformation::getDescription,
                        Transformation::setDescription);
                concatProperty(subTransformation, Transformation::getUid, Transformation::setUid);

                Optional<SlotSharingGroup> ssg = transformation.getSlotSharingGroup();
                if (ssg.isPresent() && !subTransformation.getSlotSharingGroup().isPresent()) {
                    transformation.setSlotSharingGroup(ssg.get());
                }
                subTransformation.setParallelism(transformation.getParallelism());
            }
            return result;
        }

        private void concatProperty(
                Transformation<?> subTransformation,
                Function<Transformation<?>, String> getter,
                BiConsumer<Transformation<?>, String> setter) {
            if (getter.apply(transformation) != null && getter.apply(subTransformation) != null) {
                setter.accept(
                        transformation,
                        getter.apply(transformation) + ": " + getter.apply(subTransformation));
            }
        }
    }
}
