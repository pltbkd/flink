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

package org.apache.flink.streaming.runtime.operators.sink;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.runtime.jobgraph.JobType;
import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.JobTypeAware;
import org.apache.flink.streaming.api.operators.OneInputStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;
import org.apache.flink.streaming.api.operators.YieldingOperatorFactory;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link org.apache.flink.streaming.api.operators.StreamOperatorFactory} for {@link
 * WriterOperator}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}.
 * @param <CommT> The committable type of the {@link SinkWriter}.
 * @param <WriterStateT> The type of the {@link SinkWriter Writer's} state.
 */
@Internal
public final class WriterOperatorFactory<InputT, CommT>
        extends AbstractStreamOperatorFactory<CommittableMessage<CommT>>
        implements OneInputStreamOperatorFactory<InputT, CommittableMessage<CommT>>,
                YieldingOperatorFactory<CommittableMessage<CommT>>,
                JobTypeAware {

    private final Sink<InputT> sink;
    private JobType jobType = JobType.STREAMING;

    public WriterOperatorFactory(Sink<InputT> sink) {
        this.sink = checkNotNull(sink);
    }

    public <T extends StreamOperator<CommittableMessage<CommT>>> T createStreamOperator(
            StreamOperatorParameters<CommittableMessage<CommT>> parameters) {
        try {
            final WriterOperator<InputT, CommT> writerOperator =
                    new WriterOperator<>(
                            sink,
                            processingTimeService,
                            getMailboxExecutor(),
                            jobType == JobType.BATCH);
            writerOperator.setup(
                    parameters.getContainingTask(),
                    parameters.getStreamConfig(),
                    parameters.getOutput());
            return (T) writerOperator;
        } catch (Exception e) {
            throw new IllegalStateException(
                    "Cannot create sink operator for "
                            + parameters.getStreamConfig().getOperatorName(),
                    e);
        }
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return WriterOperator.class;
    }

    @VisibleForTesting
    public Sink<InputT> getSink() {
        return sink;
    }

    @Override
    public void setJobType(JobType jobType) {
        this.jobType = jobType;
    }
}
