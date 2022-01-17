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

package org.apache.flink.connector.file.sink;

import org.apache.flink.api.common.io.FileInputFormat;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.RestOptions;
import org.apache.flink.connector.file.sink.compactor.CompactStrategy.Builder;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.IntEncoder;
import org.apache.flink.connector.file.sink.utils.IntegerFileSinkTestDataUtils.ModuloBucketAssigner;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.minicluster.MiniCluster;
import org.apache.flink.runtime.minicluster.MiniClusterConfiguration;
import org.apache.flink.streaming.api.functions.sink.filesystem.PartFileInfo;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.CheckpointRollingPolicy;
import org.apache.flink.util.TestLogger;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runners.Parameterized;

import java.io.DataInputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;

/** The base class for the File Sink IT Case in different execution mode. */
public abstract class FileSinkITBase extends TestLogger {

    protected static final int NUM_SOURCES = 4;

    protected static final int NUM_SINKS = 3;

    protected static final int NUM_RECORDS = 10000;

    protected static final int NUM_BUCKETS = 4;

    protected static final double FAILOVER_RATIO = 0.4;

    @ClassRule public static final TemporaryFolder TEMPORARY_FOLDER = new TemporaryFolder();

    @Parameterized.Parameter public boolean triggerFailover;

    @Parameterized.Parameters(name = "triggerFailover = {0}")
    public static Collection<Object[]> params() {
        return Arrays.asList(new Object[] {false}, new Object[] {true});
    }

    @Test
    public void testFileSink() throws Exception {
        String path = TEMPORARY_FOLDER.newFolder().getAbsolutePath();

        JobGraph jobGraph = createJobGraph(path);

        final Configuration config = new Configuration();
        config.setString(RestOptions.BIND_PORT, "18081-19000");
        final MiniClusterConfiguration cfg =
                new MiniClusterConfiguration.Builder()
                        .setNumTaskManagers(1)
                        .setNumSlotsPerTaskManager(4)
                        .setConfiguration(config)
                        .build();

        try (MiniCluster miniCluster = new MiniCluster(cfg)) {
            miniCluster.start();
            miniCluster.executeJobBlocking(jobGraph);
        }

        IntegerFileSinkTestDataUtils.checkIntegerSequenceSinkOutput(
                path, NUM_RECORDS, NUM_BUCKETS, NUM_SOURCES);
    }

    protected abstract JobGraph createJobGraph(String path);

    public static class IntInputFormat extends FileInputFormat<Integer> {
        private DataInputStream intStream;

        @Override
        public void open(FileInputSplit fileSplit) throws IOException {
            super.open(fileSplit);
            this.intStream = new DataInputStream(stream);
        }

        private int lastAvail;

        @Override
        public boolean reachedEnd() throws IOException {
            lastAvail = intStream.available();
            return lastAvail <= 0;
        }

        public static final Object lock = new Object();

        @Override
        public Integer nextRecord(Integer reuse) throws IOException {
            synchronized (lock) {
                int availBefore = -1;
                int availAfter = -1;
                try {
                    availBefore = intStream.available();
                    int elem = intStream.readInt();
                    availAfter = intStream.available();
                    return elem;
                } catch (IOException e) {
                    throw e;
                }
            }
        }

        @Override
        public void close() throws IOException {
            super.close();
        }
    }

    protected FileSink<Integer> createFileSink(String path) {
        return FileSink.forRowFormat(new Path(path), new IntEncoder())
                .withBucketAssigner(new ModuloBucketAssigner(NUM_BUCKETS))
                .withRollingPolicy(new PartSizeAndCheckpointRollingPolicy(1024))
                .enableGeneralCompacting(
                        Builder.newBuilder().withSizeThreshold(32 * 1024 * 1024).build(),
                        IntInputFormat::new)
                .build();
    }

    private static class PartSizeAndCheckpointRollingPolicy
            extends CheckpointRollingPolicy<Integer, String> {

        private final long maxPartSize;

        public PartSizeAndCheckpointRollingPolicy(long maxPartSize) {
            this.maxPartSize = maxPartSize;
        }

        @Override
        public boolean shouldRollOnEvent(PartFileInfo<String> partFileState, Integer element)
                throws IOException {
            return partFileState.getSize() >= maxPartSize;
        }

        @Override
        public boolean shouldRollOnProcessingTime(
                PartFileInfo<String> partFileState, long currentTime) {
            return false;
        }
    }
}
