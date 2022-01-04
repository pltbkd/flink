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

package org.apache.flink.streaming.runtime.operators.sink.committables;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.sink2.CommittableMessageSerializer;
import org.apache.flink.api.connector.sink2.CommittableSummary;
import org.apache.flink.core.io.SimpleVersionedSerialization;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector.Checkpoint;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector.Request;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector.RequestState;
import org.apache.flink.streaming.runtime.operators.sink.committables.CommittableCollector.Subtask;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The serializer for the {@link CommittableCollector}. Compatible to 1.14- StreamingCommitterState.
 */
@Internal
public final class CommittableCollectorSerializer<CommT>
        implements SimpleVersionedSerializer<CommittableCollector<CommT>> {

    private static final int MAGIC_NUMBER = 0xb91f252c;

    private final SimpleVersionedSerializer<CommT> committableSerializer;
    private final CommittableMessageSerializer<CommT> messageSerializer;

    public CommittableCollectorSerializer(SimpleVersionedSerializer<CommT> committableSerializer) {
        this.committableSerializer = checkNotNull(committableSerializer);
        messageSerializer = new CommittableMessageSerializer<>(committableSerializer);
    }

    @Override
    public int getVersion() {
        return 2;
    }

    @Override
    public byte[] serialize(CommittableCollector<CommT> committableCollector) throws IOException {
        DataOutputSerializer out = new DataOutputSerializer(256);
        out.writeInt(MAGIC_NUMBER);
        serializeV2(committableCollector, out);
        return out.getCopyOfBuffer();
    }

    @Override
    public CommittableCollector<CommT> deserialize(int version, byte[] serialized)
            throws IOException {
        final DataInputDeserializer in = new DataInputDeserializer(serialized);
        if (version == 1) {
            validateMagicNumber(in);
            return deserializeV1(in);
        }
        if (version == 2) {
            validateMagicNumber(in);
            return deserializeV2(in);
        }
        throw new IOException("Unrecognized version or corrupt state: " + version);
    }

    private CommittableCollector<CommT> deserializeV1(DataInputView in) throws IOException {
        final List<CommT> r = new ArrayList<>();
        final int committableSerializerVersion = in.readInt();
        final int numOfCommittable = in.readInt();

        for (int i = 0; i < numOfCommittable; i++) {
            final byte[] bytes = new byte[in.readInt()];
            in.readFully(bytes);
            final CommT committable =
                    committableSerializer.deserialize(committableSerializerVersion, bytes);
            r.add(committable);
        }

        return CommittableCollector.ofLegacy(r);
    }

    private void serializeV2(
            CommittableCollector<CommT> committableCollector, DataOutputView dataOutputView)
            throws IOException {

        SimpleVersionedSerialization.writeVersionAndSerializeList(
                new CheckpointSimpleVersionedSerializer(),
                new ArrayList<>(committableCollector.getCheckpoints()),
                dataOutputView);
    }

    private CommittableCollector<CommT> deserializeV2(DataInputDeserializer in) throws IOException {
        List<Checkpoint<CommT>> checkpoints =
                SimpleVersionedSerialization.readVersionAndDeserializeList(
                        new CheckpointSimpleVersionedSerializer(), in);
        return new CommittableCollector<>(
                checkpoints.stream().collect(Collectors.toMap(e -> e.getCheckpointId(), e -> e)));
    }

    private static void validateMagicNumber(DataInputView in) throws IOException {
        final int magicNumber = in.readInt();
        if (magicNumber != MAGIC_NUMBER) {
            throw new IOException(
                    String.format("Corrupt data: Unexpected magic number %08X", magicNumber));
        }
    }

    private class CheckpointSimpleVersionedSerializer
            implements SimpleVersionedSerializer<Checkpoint<CommT>> {
        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Checkpoint<CommT> checkpoint) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            out.writeLong(checkpoint.getCheckpointId());
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    new SubtaskSimpleVersionedSerializer(),
                    new ArrayList<>(checkpoint.getSubtasks()),
                    out);
            return out.getCopyOfBuffer();
        }

        @Override
        public Checkpoint<CommT> deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            long checkpointId = in.readLong();
            List<Subtask<CommT>> subtasks =
                    SimpleVersionedSerialization.readVersionAndDeserializeList(
                            new SubtaskSimpleVersionedSerializer(), in);
            return new Checkpoint<>(
                    subtasks.stream()
                            .collect(Collectors.toMap(e -> e.getSummary().getSubtaskId(), e -> e)),
                    checkpointId);
        }
    }

    private class SubtaskSimpleVersionedSerializer
            implements SimpleVersionedSerializer<Subtask<CommT>> {

        @Override
        public int getVersion() {
            return 0;
        }

        @Override
        public byte[] serialize(Subtask<CommT> subtask) throws IOException {
            DataOutputSerializer out = new DataOutputSerializer(256);
            SimpleVersionedSerialization.writeVersionAndSerialize(
                    messageSerializer, subtask.getSummary(), out);
            SimpleVersionedSerialization.writeVersionAndSerializeList(
                    new RequestSimpleVersionedSerializer(),
                    new ArrayList<>(subtask.getRequests()),
                    out);
            out.writeInt(subtask.getNumDrained());
            return out.getCopyOfBuffer();
        }

        @Override
        public Subtask<CommT> deserialize(int version, byte[] serialized) throws IOException {
            DataInputDeserializer in = new DataInputDeserializer(serialized);
            CommittableSummary<CommT> summary =
                    (CommittableSummary<CommT>)
                            SimpleVersionedSerialization.readVersionAndDeSerialize(
                                    messageSerializer, in);
            List<Request<CommT>> requests =
                    SimpleVersionedSerialization.readVersionAndDeserializeList(
                            new RequestSimpleVersionedSerializer(), in);
            return new Subtask<>(summary, requests, in.readInt());
        }

        private class RequestSimpleVersionedSerializer
                implements SimpleVersionedSerializer<Request<CommT>> {
            @Override
            public int getVersion() {
                return 0;
            }

            @Override
            public byte[] serialize(Request<CommT> request) throws IOException {
                DataOutputSerializer out = new DataOutputSerializer(256);
                SimpleVersionedSerialization.writeVersionAndSerialize(
                        committableSerializer, request.getCommittable(), out);
                out.writeInt(request.getNumberOfRetries());
                out.writeInt(request.getState().ordinal());
                return out.getCopyOfBuffer();
            }

            @Override
            public Request<CommT> deserialize(int version, byte[] serialized) throws IOException {
                DataInputDeserializer in = new DataInputDeserializer(serialized);
                CommT committable =
                        SimpleVersionedSerialization.readVersionAndDeSerialize(
                                committableSerializer, in);
                return new Request<>(
                        committable, in.readInt(), RequestState.values()[in.readInt()]);
            }
        }
    }
}
