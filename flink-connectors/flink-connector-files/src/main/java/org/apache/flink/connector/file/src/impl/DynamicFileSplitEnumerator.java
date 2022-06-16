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

package org.apache.flink.connector.file.src.impl;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.connector.source.SourceEvent;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.connector.file.src.FileSource;
import org.apache.flink.connector.file.src.FileSourceSplit;
import org.apache.flink.connector.file.src.PendingSplitsCheckpoint;
import org.apache.flink.connector.file.src.assigners.FileSplitAssigner;
import org.apache.flink.connector.file.src.enumerate.DynamicFileEnumerator;
import org.apache.flink.connector.file.src.enumerate.FileEnumerator;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.connector.source.DynamicPartitionEvent;
import org.apache.flink.util.FlinkRuntimeException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A SplitEnumerator implementation for bounded / batch {@link FileSource} input.
 *
 * <p>This enumerator takes all files that are present in the configured input directories and
 * assigns them to the readers. Once all files are processed, the source is finished.
 *
 * <p>The implementation of this class is rather thin. The actual logic for creating the set of
 * FileSourceSplits to process, and the logic to decide which reader gets what split, are in {@link
 * FileEnumerator} and in {@link FileSplitAssigner}, respectively.
 */
@Internal
public abstract class DynamicFileSplitEnumerator<SplitT extends FileSourceSplit>
        implements SplitEnumerator<SplitT, PendingSplitsCheckpoint<SplitT>> {

    private static final Logger LOG = LoggerFactory.getLogger(DynamicFileSplitEnumerator.class);

    private final SplitEnumeratorContext<SplitT> context;

    private final DynamicFileEnumerator.Provider fileEnumeratorFactory;

    private final FileSplitAssigner.Provider splitAssignerFactory;
    private transient FileSplitAssigner splitAssigner;

    private final LinkedHashMap<Integer, String> readersAwaitingSplit;

    private transient boolean partitionDataReceived;

    // ------------------------------------------------------------------------

    public DynamicFileSplitEnumerator(
            SplitEnumeratorContext<SplitT> context,
            DynamicFileEnumerator.Provider fileEnumeratorFactory,
            FileSplitAssigner.Provider splitAssignerFactory) {
        this.context = checkNotNull(context);
        this.splitAssignerFactory = checkNotNull(splitAssignerFactory);
        this.fileEnumeratorFactory = checkNotNull(fileEnumeratorFactory);
        this.partitionDataReceived = false;
        this.readersAwaitingSplit = new LinkedHashMap<>();
    }

    @Override
    public void start() {
        // no resources to start
    }

    @Override
    public void close() throws IOException {
        // no resources to close
    }

    @Override
    public void addReader(int subtaskId) {
        // this source is purely lazy-pull-based, nothing to do upon registration
    }

    @Override
    public void handleSplitRequest(int subtask, @Nullable String hostname) {
        if (!partitionDataReceived) {
            readersAwaitingSplit.put(subtask, hostname);
            return;
        }
        if (!context.registeredReaders().containsKey(subtask)) {
            // reader failed between sending the request and now. skip this request.
            return;
        }

        if (LOG.isInfoEnabled()) {
            final String hostInfo =
                    hostname == null ? "(no host locality info)" : "(on host '" + hostname + "')";
            LOG.info("Subtask {} {} is requesting a file source split", subtask, hostInfo);
        }

        final Optional<FileSourceSplit> nextSplit = splitAssigner.getNext(hostname);
        if (nextSplit.isPresent()) {
            final FileSourceSplit split = nextSplit.get();
            context.assignSplit((SplitT) split, subtask);
            LOG.info("Assigned split to subtask {} : {}", subtask, split);
        } else {
            context.signalNoMoreSplits(subtask);
            LOG.info("No more splits available for subtask {}", subtask);
        }
    }

    @Override
    public void handleSourceEvent(int subtaskId, SourceEvent sourceEvent) {
        if (sourceEvent instanceof DynamicPartitionEvent) {
            LOG.info("Received DynamicPartitionEvent: {}", subtaskId);
            DynamicFileEnumerator fileEnumerator = fileEnumeratorFactory.create();
            fileEnumerator.setPartitionData(((DynamicPartitionEvent) sourceEvent).getData());

            Collection<FileSourceSplit> splits;
            try {
                splits = fileEnumerator.enumerateSplits(new Path[1], context.currentParallelism());
            } catch (IOException e) {
                throw new FlinkRuntimeException("Could not enumerate file splits", e);
            }
            splitAssigner = splitAssignerFactory.create(splits);
            this.partitionDataReceived = true;
            assignAwaitingRequest();
        } else {
            LOG.error("Received unrecognized event: {}", sourceEvent);
        }
    }

    @Override
    public void addSplitsBack(List<SplitT> splits, int subtaskId) {
        LOG.debug("File Source Enumerator adds splits back: {}", splits);
        List<FileSourceSplit> fileSplits = new ArrayList<>(splits);
        splitAssigner.addSplits(fileSplits);
    }

    @Override
    public PendingSplitsCheckpoint<SplitT> snapshotState(long checkpointId) {
        // do not do snapshot
        return PendingSplitsCheckpoint.fromCollectionSnapshot(new ArrayList<>());
    }

    private void assignAwaitingRequest() {
        final Iterator<Map.Entry<Integer, String>> awaitingReader =
                readersAwaitingSplit.entrySet().iterator();

        while (awaitingReader.hasNext()) {
            final Map.Entry<Integer, String> nextAwaiting = awaitingReader.next();

            // if the reader that requested another split has failed in the meantime, remove
            // it from the list of waiting readers
            if (!context.registeredReaders().containsKey(nextAwaiting.getKey())) {
                awaitingReader.remove();
                continue;
            }

            final String hostname = nextAwaiting.getValue();
            final int awaitingSubtask = nextAwaiting.getKey();
            handleSplitRequest(awaitingSubtask, hostname);
        }
    }
}
