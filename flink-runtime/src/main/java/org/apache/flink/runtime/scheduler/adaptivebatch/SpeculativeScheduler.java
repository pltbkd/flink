/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.runtime.scheduler.adaptivebatch;

import org.apache.flink.api.common.time.Time;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.JobManagerOptions;
import org.apache.flink.runtime.blocklist.BlockedNode;
import org.apache.flink.runtime.blocklist.BlocklistHandler;
import org.apache.flink.runtime.checkpoint.CheckpointRecoveryFactory;
import org.apache.flink.runtime.checkpoint.CheckpointsCleaner;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.execution.ExecutionState;
import org.apache.flink.runtime.executiongraph.Execution;
import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.executiongraph.JobStatusListener;
import org.apache.flink.runtime.executiongraph.SpeculativeExecutionVertex;
import org.apache.flink.runtime.executiongraph.failover.flip1.FailoverStrategy;
import org.apache.flink.runtime.executiongraph.failover.flip1.RestartBackoffTimeStrategy;
import org.apache.flink.runtime.io.network.partition.PartitionException;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.metrics.groups.JobManagerJobMetricGroup;
import org.apache.flink.runtime.scheduler.ExecutionGraphFactory;
import org.apache.flink.runtime.scheduler.ExecutionOperations;
import org.apache.flink.runtime.scheduler.ExecutionSlotAllocatorFactory;
import org.apache.flink.runtime.scheduler.ExecutionVertexVersioner;
import org.apache.flink.runtime.scheduler.slowtaskdetector.ExecutionTimeBasedSlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetector;
import org.apache.flink.runtime.scheduler.slowtaskdetector.SlowTaskDetectorListener;
import org.apache.flink.runtime.scheduler.strategy.ExecutionVertexID;
import org.apache.flink.runtime.scheduler.strategy.SchedulingStrategyFactory;
import org.apache.flink.runtime.shuffle.ShuffleMaster;
import org.apache.flink.runtime.taskmanager.TaskManagerLocation;
import org.apache.flink.runtime.topology.Vertex;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.IterableUtils;
import org.apache.flink.util.concurrent.FutureUtils;
import org.apache.flink.util.concurrent.ScheduledExecutor;

import org.slf4j.Logger;

import javax.annotation.Nullable;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Executor;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/** The speculative scheduler. */
public class SpeculativeScheduler extends AdaptiveBatchScheduler
        implements SlowTaskDetectorListener {

    private final int maxConcurrentExecutions;

    private final Duration blockSlowNodeDuration;

    private final BlocklistHandler blocklistHandler;

    private final SlowTaskDetector slowTaskDetector;

    public SpeculativeScheduler(
            final Logger log,
            final JobGraph jobGraph,
            final Executor ioExecutor,
            final Configuration jobMasterConfiguration,
            final Consumer<ComponentMainThreadExecutor> startUpAction,
            final ScheduledExecutor delayExecutor,
            final ClassLoader userCodeLoader,
            final CheckpointsCleaner checkpointsCleaner,
            final CheckpointRecoveryFactory checkpointRecoveryFactory,
            final JobManagerJobMetricGroup jobManagerJobMetricGroup,
            final SchedulingStrategyFactory schedulingStrategyFactory,
            final FailoverStrategy.Factory failoverStrategyFactory,
            final RestartBackoffTimeStrategy restartBackoffTimeStrategy,
            final ExecutionOperations executionOperations,
            final ExecutionVertexVersioner executionVertexVersioner,
            final ExecutionSlotAllocatorFactory executionSlotAllocatorFactory,
            long initializationTimestamp,
            final ComponentMainThreadExecutor mainThreadExecutor,
            final JobStatusListener jobStatusListener,
            final ExecutionGraphFactory executionGraphFactory,
            final ShuffleMaster<?> shuffleMaster,
            final Time rpcTimeout,
            final VertexParallelismDecider vertexParallelismDecider,
            final int defaultMaxParallelism,
            final BlocklistHandler blocklistHandler)
            throws Exception {

        super(
                log,
                jobGraph,
                ioExecutor,
                jobMasterConfiguration,
                startUpAction,
                delayExecutor,
                userCodeLoader,
                checkpointsCleaner,
                checkpointRecoveryFactory,
                jobManagerJobMetricGroup,
                schedulingStrategyFactory,
                failoverStrategyFactory,
                restartBackoffTimeStrategy,
                executionOperations,
                executionVertexVersioner,
                executionSlotAllocatorFactory,
                initializationTimestamp,
                mainThreadExecutor,
                jobStatusListener,
                executionGraphFactory,
                shuffleMaster,
                rpcTimeout,
                vertexParallelismDecider,
                defaultMaxParallelism);

        this.maxConcurrentExecutions =
                jobMasterConfiguration.getInteger(
                        JobManagerOptions.SPECULATIVE_MAX_CONCURRENT_EXECUTIONS);

        this.blockSlowNodeDuration =
                jobMasterConfiguration.get(JobManagerOptions.BLOCK_SLOW_NODE_DURATION);

        this.blocklistHandler = checkNotNull(blocklistHandler);

        this.slowTaskDetector = new ExecutionTimeBasedSlowTaskDetector(jobMasterConfiguration);
    }

    @Override
    protected void startSchedulingInternal() {
        super.startSchedulingInternal();
        slowTaskDetector.start(getExecutionGraph(), this, getMainThreadExecutor());
    }

    @Override
    public CompletableFuture<Void> closeAsync() {
        CompletableFuture<Void> future = super.closeAsync();
        slowTaskDetector.stop();
        return future;
    }

    @Override
    public SpeculativeExecutionVertex getExecutionVertex(ExecutionVertexID executionVertexId) {
        return (SpeculativeExecutionVertex) super.getExecutionVertex(executionVertexId);
    }

    @Override
    protected void onTaskFinished(final Execution execution) {
        // cancel all un-terminated executions because the execution vertex has finished
        FutureUtils.assertNoException(cancelPendingExecutions(execution.getVertex().getID()));

        initializeVerticesIfPossible();

        super.onTaskFinished(execution);
    }

    private CompletableFuture<?> cancelPendingExecutions(
            final ExecutionVertexID executionVertexId) {
        final SpeculativeExecutionVertex executionVertex = getExecutionVertex(executionVertexId);

        final List<CompletableFuture<?>> cancelingFutures = new ArrayList<>();
        for (Execution execution : executionVertex.getCurrentExecutions()) {
            if (!execution.getState().isTerminal()) {
                execution.cancel();
                cancelingFutures.add(execution.getReleaseFuture());
            }
        }
        cancelAllPendingSlotRequests(executionVertexId);
        return FutureUtils.combineAll(cancelingFutures);
    }

    @Override
    protected void onTaskFailed(final Execution execution) {
        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(execution.getVertex().getID());
        final ExecutionAttemptID attemptId = execution.getAttemptId();

        // when an execution fails, remove it from current executions to make room for future
        // speculative executions
        executionVertex.archiveFailedExecution(attemptId);
        executionSlotAllocator.cancel(attemptId);

        super.onTaskFailed(execution);
    }

    @Override
    protected void handleTaskFailure(
            final Execution failedExecution, @Nullable final Throwable error) {

        final SpeculativeExecutionVertex executionVertex =
                getExecutionVertex(failedExecution.getVertex().getID());

        // if the execution vertex is not possible finish or a PartitionException occurred, trigger
        // an execution vertex failover to recover
        if (!isExecutionVertexPossibleToFinish(executionVertex)
                || ExceptionUtils.findThrowable(error, PartitionException.class).isPresent()) {
            super.handleTaskFailure(failedExecution, error);
        } else {
            // add the execution failure to exception history even though not restarting the entire
            // execution vertex
            final long timestamp = System.currentTimeMillis();
            setGlobalFailureCause(error, timestamp);
            archiveFromFailureHandlingResult(
                    createFailureHandlingResultSnapshot(
                            executionFailureHandler.getFailureHandlingResult(
                                    failedExecution, error, timestamp)));
        }
    }

    private static boolean isExecutionVertexPossibleToFinish(
            final SpeculativeExecutionVertex executionVertex) {
        boolean anyExecutionPossibleToFinish = false;
        for (Execution execution : executionVertex.getCurrentExecutions()) {
            // if any execution has finished, no execution of the same execution vertex should fail
            // after that
            checkState(execution.getState() != ExecutionState.FINISHED);

            if (execution.getState() == ExecutionState.CREATED
                    || execution.getState() == ExecutionState.SCHEDULED
                    || execution.getState() == ExecutionState.DEPLOYING
                    || execution.getState() == ExecutionState.INITIALIZING
                    || execution.getState() == ExecutionState.RUNNING) {
                anyExecutionPossibleToFinish = true;
            }
        }
        return anyExecutionPossibleToFinish;
    }

    @Override
    protected void cancelAllPendingSlotRequestsInternal() {
        IterableUtils.toStream(getSchedulingTopology().getVertices())
                .map(Vertex::getId)
                .forEach(this::cancelAllPendingSlotRequests);
    }

    @Override
    protected void cancelAllPendingSlotRequestsForVertices(
            final Set<ExecutionVertexID> executionVertices) {
        executionVertices.forEach(this::cancelAllPendingSlotRequests);
    }

    private void cancelAllPendingSlotRequests(final ExecutionVertexID executionVertexId) {
        final SpeculativeExecutionVertex executionVertex = getExecutionVertex(executionVertexId);
        executionVertex
                .getCurrentExecutions()
                .forEach(e -> executionSlotAllocator.cancel(e.getAttemptId()));
    }

    @Override
    public void notifySlowTasks(Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks) {
        // add slow nodes to blocklist before scheduling new speculative executions
        final long blockedEndTimestamp =
                System.currentTimeMillis() + blockSlowNodeDuration.toMillis();
        final Collection<BlockedNode> nodesToBlock =
                getSlowNodeIds(slowTasks).stream()
                        .map(
                                nodeId ->
                                        new BlockedNode(
                                                nodeId,
                                                "Node is detected to be slow.",
                                                blockedEndTimestamp))
                        .collect(Collectors.toList());
        blocklistHandler.addNewBlockedNodes(nodesToBlock);

        final List<Execution> newSpeculativeExecutions = new ArrayList<>();
        final Set<ExecutionVertexID> verticesToDeploy = new HashSet<>();
        for (ExecutionVertexID executionVertexId : slowTasks.keySet()) {
            final SpeculativeExecutionVertex executionVertex =
                    getExecutionVertex(executionVertexId);

            if (executionVertex.containsSources() || executionVertex.containsSinks()) {
                continue;
            }

            final int currentConcurrentExecutions = executionVertex.getCurrentExecutions().size();
            final int newSpeculativeExecutionsToDeploy =
                    maxConcurrentExecutions - currentConcurrentExecutions;
            if (newSpeculativeExecutionsToDeploy > 0) {
                log.info(
                        "{} ({}) is detected as a slow vertex, create and deploy {} new speculative executions for it.",
                        executionVertex.getTaskNameWithSubtaskIndex(),
                        executionVertex.getID(),
                        newSpeculativeExecutionsToDeploy);

                verticesToDeploy.add(executionVertexId);
                IntStream.range(0, newSpeculativeExecutionsToDeploy)
                        .mapToObj(executionVertex::createNewSpeculativeExecution)
                        .forEach(newSpeculativeExecutions::add);
            }
        }

        executionDeployer.allocateSlotsAndDeploy(
                newSpeculativeExecutions,
                executionVertexVersioner.getExecutionVertexVersions(verticesToDeploy));
    }

    private Set<String> getSlowNodeIds(
            Map<ExecutionVertexID, Collection<ExecutionAttemptID>> slowTasks) {
        final Set<ExecutionAttemptID> slowExecutions =
                slowTasks.values().stream()
                        .flatMap(ids -> ids.stream())
                        .collect(Collectors.toSet());

        return slowExecutions.stream()
                .map(id -> getExecutionGraph().getRegisteredExecutions().get(id))
                .map(Execution::getAssignedResourceLocation)
                .map(TaskManagerLocation::getNodeId)
                .collect(Collectors.toSet());
    }
}
