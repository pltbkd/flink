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

package org.apache.flink.table.tpcds.plan;

import org.apache.flink.api.dag.Transformation;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.jobgraph.JobVertex;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.graph.StreamGraph;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.config.ExecutionConfigOptions;
import org.apache.flink.table.api.config.OptimizerConfigOptions;
import org.apache.flink.table.api.internal.TableEnvironmentImpl;
import org.apache.flink.table.catalog.hive.HiveCatalog;
import org.apache.flink.table.operations.CollectModifyOperation;
import org.apache.flink.table.operations.Operation;
import org.apache.flink.table.operations.QueryOperation;

import org.apache.hive.common.util.HiveVersionInfo;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.tpcds.TpcdsTestProgram.loadFile2String;

public class PlannerTest {
    private static final String QUERY_PREFIX = "query";
    private static final String QUERY_SUFFIX = ".sql";

    public static void main(String[] args) throws Exception {
        EnvironmentSettings settings = EnvironmentSettings.newInstance().inBatchMode().build();
        TableEnvironment tEnv = TableEnvironment.create(settings);

        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_JOIN_REORDER_ENABLED, true);
        tEnv.getConfig()
                .getConfiguration()
                .setBoolean(OptimizerConfigOptions.TABLE_OPTIMIZER_REUSE_SOURCE_ENABLED, false);
        tEnv.getConfig()
                .getConfiguration()
                .setLong(
                        OptimizerConfigOptions.TABLE_OPTIMIZER_BROADCAST_JOIN_THRESHOLD, 10485760L);
        tEnv.getConfig()
                .getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_RESOURCE_DEFAULT_PARALLELISM, 4);
        tEnv.getConfig()
                .getConfiguration()
                .setInteger(ExecutionConfigOptions.TABLE_EXEC_SORT_DEFAULT_LIMIT, 200);

        // Register a hive catalog
        HiveCatalog catalog =
                new HiveCatalog(
                        "hive",
                        "tpcds_bin_partitioned_orc_5",
                        "/Users/owner-pc/workspace_commu/flink/flink-end-to-end-tests/flink-tpcds-test/src/test/resources",
                        HiveVersionInfo.getVersion());
        tEnv.registerCatalog("hive", catalog);
        tEnv.useCatalog("hive");

        // Load a query
        String queryName = QUERY_PREFIX + 7 + QUERY_SUFFIX;
        String queryFilePath =
                "/Users/owner-pc/workspace_commu/flink/flink-end-to-end-tests/flink-tpcds-test/tpcds-tool/query/"
                        + queryName;
        String queryString = loadFile2String(queryFilePath);
        // System.out.println(tEnv.explainSql(queryString, ExplainDetail.ESTIMATED_COST));

        TableEnvironmentImpl tableEnvironment = (TableEnvironmentImpl) tEnv;
        List<Operation> operations = tableEnvironment.getParser().parse(queryString);

        QueryOperation queryOperation = (QueryOperation) operations.get(0);

        CollectModifyOperation sinkOperation = new CollectModifyOperation(queryOperation);
        List<Transformation<?>> transformations =
                tableEnvironment.getPlanner().translate(Collections.singletonList(sinkOperation));
        StreamExecutionEnvironment env =
                new StreamExecutionEnvironment(tableEnvironment.getConfig().getConfiguration());
        env.getTransformations().addAll(transformations);
        StreamGraph streamGraph = env.getStreamGraph();
        System.out.println(streamGraph.getStreamingPlanAsJSON());

        JobGraph jobGraph = streamGraph.getJobGraph();
        System.out.println("\n\n");
        for (JobVertex vertex : jobGraph.getVerticesSortedTopologicallyFromSources()) {
            System.out.println("JobVertex" + ":" + vertex);
        }
    }
}
