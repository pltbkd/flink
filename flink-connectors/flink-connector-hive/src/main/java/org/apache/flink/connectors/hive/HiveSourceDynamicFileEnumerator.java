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

package org.apache.flink.connectors.hive;

import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.PartitionData;
import org.apache.flink.types.Row;

import org.apache.hadoop.mapred.JobConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import static org.apache.flink.connectors.hive.HiveSourceFileEnumerator.createInputSplits;

/** HiveSourceDynamicFileEnumerator. */
public class HiveSourceDynamicFileEnumerator implements DynamicFileEnumerator {

    private static final Logger LOG =
            LoggerFactory.getLogger(HiveSourceDynamicFileEnumerator.class);

    private final String table;
    private final List<String> dynamicPartitionKeys;
    // For non-partition hive table, partitions only contains one partition which partitionValues is
    // empty.
    private final List<HiveTablePartition> partitions;
    private List<HiveTablePartition> finalPartitions;
    private final int threadNum;
    private final JobConf jobConf;

    public HiveSourceDynamicFileEnumerator(
            String table,
            List<String> dynamicPartitionKeys,
            List<HiveTablePartition> partitions,
            int threadNum,
            JobConf jobConf) {
        this.table = table;
        this.dynamicPartitionKeys = dynamicPartitionKeys;
        this.partitions = partitions;
        this.threadNum = threadNum;
        this.jobConf = jobConf;
    }

    public void setPartitionData(PartitionData partitionData) {
        LOG.info("Table: {}, Partition Data: {}", table, partitionData);
        finalPartitions = new ArrayList<>();
        for (HiveTablePartition partition : partitions) {
            Object[] values =
                    dynamicPartitionKeys.stream()
                            .map(k -> partition.getPartitionSpec().get(k))
                            .toArray(String[]::new);
            if (partitionData.hasPartition(Row.of(values))) {
                finalPartitions.add(partition);
            }
        }
        LOG.info(
                "Table: {}, Original partition number: {}, Remaining partition number: {}",
                table,
                partitions.size(),
                finalPartitions.size());
    }

    @Override
    public Collection<FileSourceSplit> enumerateSplits(Path[] paths, int minDesiredSplits)
            throws IOException {
        return new ArrayList<>(
                createInputSplits(minDesiredSplits, finalPartitions, threadNum, jobConf));
    }

    /** A factory to create {@link HiveSourceDynamicFileEnumerator}. */
    public static class Provider implements DynamicFileEnumerator.Provider {

        private static final long serialVersionUID = 1L;

        private final String table;
        private final List<String> dynamicPartitionKeys;
        private final List<HiveTablePartition> partitions;
        private final int threadNum;
        private final JobConfWrapper jobConfWrapper;

        public Provider(
                String table,
                List<String> dynamicPartitionKeys,
                List<HiveTablePartition> partitions,
                int threadNum,
                JobConfWrapper jobConfWrapper) {
            this.table = table;
            this.dynamicPartitionKeys = dynamicPartitionKeys;
            this.partitions = partitions;
            this.threadNum = threadNum;
            this.jobConfWrapper = jobConfWrapper;
        }

        @Override
        public DynamicFileEnumerator create() {
            return new HiveSourceDynamicFileEnumerator(
                    table, dynamicPartitionKeys, partitions, threadNum, jobConfWrapper.conf());
        }
    }
}
