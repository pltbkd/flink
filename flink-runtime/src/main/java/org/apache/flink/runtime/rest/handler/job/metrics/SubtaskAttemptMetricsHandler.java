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

package org.apache.flink.runtime.rest.handler.job.metrics;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.runtime.jobgraph.JobVertexID;
import org.apache.flink.runtime.rest.handler.HandlerRequest;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricFetcher;
import org.apache.flink.runtime.rest.handler.legacy.metrics.MetricStore;
import org.apache.flink.runtime.rest.messages.EmptyRequestBody;
import org.apache.flink.runtime.rest.messages.JobIDPathParameter;
import org.apache.flink.runtime.rest.messages.JobVertexIdPathParameter;
import org.apache.flink.runtime.rest.messages.SubtaskIndexPathParameter;
import org.apache.flink.runtime.rest.messages.job.SubtaskAttemptPathParameter;
import org.apache.flink.runtime.rest.messages.job.metrics.SubtaskAttemptMetricsHeaders;
import org.apache.flink.runtime.rest.messages.job.metrics.SubtaskAttemptMetricsMessageParameters;
import org.apache.flink.runtime.webmonitor.RestfulGateway;
import org.apache.flink.runtime.webmonitor.retriever.GatewayRetriever;

import javax.annotation.Nullable;

import java.util.Map;

/**
 * Handler that returns subtask metrics.
 *
 * @see MetricStore#getSubtaskAttemptMetricStore(String, String, int, int)
 */
public class SubtaskAttemptMetricsHandler
        extends AbstractMetricsHandler<SubtaskAttemptMetricsMessageParameters> {

    public SubtaskAttemptMetricsHandler(
            GatewayRetriever<? extends RestfulGateway> leaderRetriever,
            Time timeout,
            Map<String, String> headers,
            MetricFetcher metricFetcher) {

        super(
                leaderRetriever,
                timeout,
                headers,
                SubtaskAttemptMetricsHeaders.getInstance(),
                metricFetcher);
    }

    @Nullable
    @Override
    protected MetricStore.ComponentMetricStore getComponentMetricStore(
            HandlerRequest<EmptyRequestBody> request, MetricStore metricStore) {

        final JobID jobId = request.getPathParameter(JobIDPathParameter.class);
        final JobVertexID vertexId = request.getPathParameter(JobVertexIdPathParameter.class);
        final int subtaskIndex = request.getPathParameter(SubtaskIndexPathParameter.class);
        final int attemptNum = request.getPathParameter(SubtaskAttemptPathParameter.class);

        return metricStore.getSubtaskAttemptMetricStore(
                jobId.toString(), vertexId.toString(), subtaskIndex, attemptNum);
    }
}
