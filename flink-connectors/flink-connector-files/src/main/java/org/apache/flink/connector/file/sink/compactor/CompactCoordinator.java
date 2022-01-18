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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.typeutils.base.array.BytePrimitiveArraySerializer;
import org.apache.flink.api.connector.sink2.CommittableMessage;
import org.apache.flink.api.connector.sink2.CommittableMessageSerializer;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.runtime.state.StateInitializationContext;
import org.apache.flink.runtime.state.StateSnapshotContext;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.BoundedOneInput;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.util.SimpleVersionedListState;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;

import static org.apache.flink.util.Preconditions.checkNotNull;

public class CompactCoordinator<CommT, CompT> extends AbstractStreamOperator<CompT>
        implements OneInputStreamOperator<CommittableMessage<CommT>, CompT>, BoundedOneInput {

    private final CompactRequestPacker<CommT, CompT> packer;
    private final SimpleVersionedSerializer<CommT> committableSerializer;

    private static final ListStateDescriptor<byte[]> REMAINING_COMPACT_COMMT_RAW_STATES_DESC =
            new ListStateDescriptor<>(
                    "remaining_compact_commt_raw_state", BytePrimitiveArraySerializer.INSTANCE);

    private ListState<CommittableMessage<CommT>> remainingCommittableState;

    public CompactCoordinator(
            CompactRequestPacker<CommT, CompT> packer,
            SimpleVersionedSerializer<CommT> committableSerializer) {
        this.packer = checkNotNull(packer);
        this.committableSerializer = checkNotNull(committableSerializer);
    }

    @Override
    public void processElement(StreamRecord<CommittableMessage<CommT>> element) throws Exception {
        packer.accept(element.getValue());
        emitRequests();
    }

    @Override
    public void endInput() throws Exception {
        // emit all requests remained
        packer.endInput();
        emitRequests();
    }

    @Override
    public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
        // emit timeout requests, TODO use processing time service?
        super.prepareSnapshotPreBarrier(checkpointId);
        emitRequests();
    }

    private void emitRequests() {
        CompT request;
        while ((request = packer.getPackedRequest()) != null) {
            output.collect(new StreamRecord<>(request));
        }
    }

    @Override
    public void snapshotState(StateSnapshotContext context) throws Exception {
        super.snapshotState(context);
        remainingCommittableState.update(packer.getRemaining());
    }

    @Override
    public void initializeState(StateInitializationContext context) throws Exception {
        super.initializeState(context);

        remainingCommittableState =
                new SimpleVersionedListState<>(
                        context.getOperatorStateStore()
                                .getListState(REMAINING_COMPACT_COMMT_RAW_STATES_DESC),
                        new CommittableMessageSerializer<>(committableSerializer));

        Iterable<CommittableMessage<CommT>> stateRemaining = remainingCommittableState.get();
        if (stateRemaining != null) {
            for (CommittableMessage<CommT> committable : stateRemaining) {
                // restore, and redistribute when parallelism of compactor is changed
                packer.accept(committable);
            }
        }
    }
}
