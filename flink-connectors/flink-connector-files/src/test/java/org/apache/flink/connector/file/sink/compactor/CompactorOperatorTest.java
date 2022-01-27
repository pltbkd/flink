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

import org.apache.flink.connector.file.sink.FileSinkCommittable;
import org.apache.flink.connector.file.sink.FileSinkCommittableSerializer;
import org.apache.flink.connector.file.sink.compactor.DecoderBasedReader.Decoder;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorOperator;
import org.apache.flink.connector.file.sink.compactor.operator.CompactorRequest;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestInProgressFileRecoverable;
import org.apache.flink.connector.file.sink.utils.FileSinkTestUtils.TestPendingFileRecoverable;
import org.apache.flink.core.fs.Path;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.streaming.api.connector.sink2.CommittableMessage;
import org.apache.flink.streaming.api.connector.sink2.CommittableSummary;
import org.apache.flink.streaming.api.connector.sink2.CommittableWithLineage;
import org.apache.flink.streaming.api.connector.sink2.SinkV2Assertions;
import org.apache.flink.streaming.api.functions.sink.filesystem.BucketWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.InProgressFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.InProgressFileWriter.PendingFileRecoverable;
import org.apache.flink.streaming.api.functions.sink.filesystem.WriterProperties;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.util.OneInputStreamOperatorTestHarness;

import org.junit.Assert;
import org.junit.Test;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

/** Test for {@link CompactorOperator}. */
public class CompactorOperatorTest extends AbstractCompactTestBase {

    @Test
    public void testCompact() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(
                        new DecoderBasedReader.Factory<>((Decoder<Integer>) InputStream::read));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            harness.processElement(
                    request(
                            "0",
                            Arrays.asList(committable("0", ".0", 5), committable("0", ".1", 5)),
                            null));

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1L);
            harness.notifyOfCompletedCheckpoint(1);

            compactor.getAllTasksFuture().join();

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(2);

            // 1summary+1compacted+2cleanup
            List<CommittableMessage<FileSinkCommittable>> results = harness.extractOutputValues();
            Assert.assertEquals(4, results.size());
            SinkV2Assertions.assertThat((CommittableSummary<?>) results.get(0))
                    .hasPendingCommittables(3);
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(1))
                    .hasCommittable(committable("0", "compacted-0", 10));
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(2))
                    .hasCommittable(cleanupPath("0", ".0"));
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(3))
                    .hasCommittable(cleanupPath("0", ".1"));
        }
    }

    @Test
    public void testPassthrough() throws Exception {
        FileCompactor fileCompactor =
                new RecordWiseFileCompactor<>(
                        new DecoderBasedReader.Factory<>((Decoder<Integer>) InputStream::read));
        CompactorOperator compactor = createTestOperator(fileCompactor);

        try (OneInputStreamOperatorTestHarness<
                        CompactorRequest, CommittableMessage<FileSinkCommittable>>
                harness = new OneInputStreamOperatorTestHarness<>(compactor)) {
            harness.setup();
            harness.open();

            FileSinkCommittable passThroughCommittable = committable("0", "0", 5);
            FileSinkCommittable cleanupInprogressRequest = cleanupInprogress("0", "1", 1);
            FileSinkCommittable cleanupPathRequest = cleanupPath("0", "2");

            harness.processElement(
                    request("0", Collections.singletonList(passThroughCommittable), null));
            harness.processElement(
                    request("0", null, Collections.singletonList(cleanupInprogressRequest)));
            harness.processElement(
                    request("0", null, Collections.singletonList(cleanupPathRequest)));

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(1);
            harness.snapshot(1, 1L);
            harness.notifyOfCompletedCheckpoint(1);

            compactor.getAllTasksFuture().join();

            Assert.assertEquals(0, harness.extractOutputValues().size());

            harness.prepareSnapshotPreBarrier(2);

            List<CommittableMessage<FileSinkCommittable>> results = harness.extractOutputValues();
            Assert.assertEquals(4, results.size());
            SinkV2Assertions.assertThat((CommittableSummary<?>) results.get(0))
                    .hasPendingCommittables(3);
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(1))
                    .hasCommittable(passThroughCommittable);
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(2))
                    .hasCommittable(cleanupInprogressRequest);
            SinkV2Assertions.assertThat((CommittableWithLineage<?>) results.get(3))
                    .hasCommittable(cleanupPathRequest);
        }
    }

    private StreamRecord<CompactorRequest> request(
            String bucketId,
            List<FileSinkCommittable> toCompact,
            List<FileSinkCommittable> toPassthrough) {
        return new StreamRecord<>(
                new CompactorRequest(
                        bucketId,
                        toCompact == null ? new ArrayList<>() : toCompact,
                        toPassthrough == null ? new ArrayList<>() : toPassthrough),
                0L);
    }

    private FileSinkCommittable committable(String bucketId, String name, int size)
            throws IOException {
        // put bucketId after name to keep the possible '.' prefix in name
        return new FileSinkCommittable(
                bucketId,
                new TestPendingFileRecoverable(
                        newFile(name + "_" + bucketId, size <= 0 ? 1 : size), size));
    }

    private FileSinkCommittable cleanupInprogress(String bucketId, String name, int size)
            throws IOException {
        Path toCleanup = newFile(name + "_" + bucketId, size);
        return new FileSinkCommittable(
                bucketId, new TestInProgressFileRecoverable(toCleanup, size));
    }

    private FileSinkCommittable cleanupPath(String bucketId, String name) throws IOException {
        Path toCleanup = newFile(name + "_" + bucketId, 1);
        return new FileSinkCommittable(bucketId, toCleanup);
    }

    private SimpleVersionedSerializer<FileSinkCommittable> getTestCommittableSerializer() {
        return new FileSinkCommittableSerializer(
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        TestPendingFileRecoverable::new),
                new FileSinkTestUtils.SimpleVersionedWrapperSerializer<>(
                        TestInProgressFileRecoverable::new));
    }

    private CompactorOperator createTestOperator(FileCompactor compactor) {
        return new CompactorOperator(
                2,
                getTestCommittableSerializer(),
                compactor,
                new BucketWriter<Integer, String>() {

                    @Override
                    public InProgressFileWriter<Integer, String> openNewInProgressFile(
                            String bucketId, Path path, long creationTime) throws IOException {
                        return new InProgressFileWriter<Integer, String>() {
                            BufferedWriter writer;
                            long size = 0L;

                            @Override
                            public void write(Integer element, long currentTime)
                                    throws IOException {
                                if (writer == null) {
                                    writer = new BufferedWriter(new FileWriter(path.toString()));
                                }
                                writer.write(element);
                                size += 1;
                            }

                            @Override
                            public InProgressFileRecoverable persist() throws IOException {
                                return new TestInProgressFileRecoverable(path, size);
                            }

                            @Override
                            public PendingFileRecoverable closeForCommit() throws IOException {
                                return new TestPendingFileRecoverable(path, size);
                            }

                            @Override
                            public void dispose() {}

                            @Override
                            public String getBucketId() {
                                return bucketId;
                            }

                            @Override
                            public long getCreationTime() {
                                return 0;
                            }

                            @Override
                            public long getSize() throws IOException {
                                return size;
                            }

                            @Override
                            public long getLastUpdateTime() {
                                return 0;
                            }
                        };
                    }

                    @Override
                    public InProgressFileWriter<Integer, String> resumeInProgressFileFrom(
                            String s,
                            InProgressFileRecoverable inProgressFileSnapshot,
                            long creationTime)
                            throws IOException {
                        return null;
                    }

                    @Override
                    public WriterProperties getProperties() {
                        return null;
                    }

                    @Override
                    public PendingFile recoverPendingFile(
                            PendingFileRecoverable pendingFileRecoverable) throws IOException {
                        return new PendingFile() {
                            @Override
                            public void commit() throws IOException {
                                TestPendingFileRecoverable testRecoverable =
                                        (TestPendingFileRecoverable) pendingFileRecoverable;
                                if (testRecoverable.getPath() != null) {
                                    if (!testRecoverable
                                            .getPath()
                                            .equals(testRecoverable.getUncommittedPath())) {
                                        testRecoverable
                                                .getPath()
                                                .getFileSystem()
                                                .rename(
                                                        testRecoverable.getUncommittedPath(),
                                                        testRecoverable.getPath());
                                    }
                                }
                            }

                            @Override
                            public void commitAfterRecovery() throws IOException {
                                commit();
                            }
                        };
                    }

                    @Override
                    public boolean cleanupInProgressFileRecoverable(
                            InProgressFileRecoverable inProgressFileRecoverable)
                            throws IOException {
                        return false;
                    }
                });
    }
}
