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

package org.apache.flink.table.planner.plan.nodes.exec.processor;

import org.apache.flink.table.planner.plan.nodes.exec.ExecEdge;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.InputProperty;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecExchange;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecMultipleInput;
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.util.Preconditions.checkState;

public class DppOmitDependencyProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        Map<ExecNode<?>, List<ExecNode<?>>> dppScanDescendants = new HashMap<>();

        AbstractExecNodeExactlyOnceVisitor dppScanCollector =
                new AbstractExecNodeExactlyOnceVisitor() {
                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        node.getInputEdges().stream()
                                .map(ExecEdge::getSource)
                                .forEach(
                                        input -> {
                                            // The character of the dpp scan is that it has an
                                            // input.
                                            if ((input instanceof BatchExecTableSourceScan
                                                            && input.getInputEdges().size() == 1)
                                                    || input instanceof BatchExecExchange) {
                                                dppScanDescendants
                                                        .computeIfAbsent(
                                                                input, ignored -> new ArrayList<>())
                                                        .add(node);
                                            }
                                        });

                        visitInputs(node);
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(dppScanCollector));

        List<ExecNode<?>> roots = new ArrayList<>(execGraph.getRootNodes());

        // Let's now check if there are chain
        for (Map.Entry<ExecNode<?>, List<ExecNode<?>>> entry : dppScanDescendants.entrySet()) {
            if (entry.getKey() instanceof BatchExecTableSourceScan) {
                if (entry.getValue().size() > 1) {
                    continue;
                }

                ExecNode<?> next = entry.getValue().get(0);
                if (next instanceof BatchExecMultipleInput) {
                    ((BatchExecTableSourceScan) entry.getKey()).setSkipDependencyEdge(true);
                    System.out.println("Set " + entry.getKey() + " to use chain 1");
                    continue;
                }

                if (next instanceof BatchExecExchange
                        && dppScanDescendants.get(next).size() == 1
                        && Arrays.asList(
                                        InputProperty.DistributionType.ANY,
                                        InputProperty.DistributionType.KEEP_INPUT_AS_IS)
                                .contains(
                                        next.getInputProperties()
                                                .get(0)
                                                .getRequiredDistribution()
                                                .getType())
                        && dppScanDescendants.get(next).get(0) instanceof BatchExecMultipleInput) {

                    BatchExecTableSourceScan source = (BatchExecTableSourceScan) entry.getKey();
                    BatchExecExchange exchange = (BatchExecExchange) next;
                    BatchExecMultipleInput multipleInput =
                            (BatchExecMultipleInput) dppScanDescendants.get(next).get(0);

                    ExecEdge edge = ExecEdge.builder().source(source).target(multipleInput).build();

                    int outEdgeIndex = findEdge(multipleInput.getInputEdges(), exchange);
                    checkState(outEdgeIndex >= 0, "The out edge not found");
                    multipleInput.replaceInputEdge(outEdgeIndex, edge);
                    System.out.println(
                            "replace outEdgeIndex "
                                    + multipleInput.getInputEdges().get(outEdgeIndex));

                    // Also change the input edge
                    replaceInnerEdge(multipleInput.getRootNode(), exchange, edge);

                    // Now move dpp sink to the root
                    roots.add(source.getInputEdges().get(0).getSource());
                    source.setInputEdges(Collections.emptyList());

                    System.out.println("Set " + entry.getKey() + " to use chain 2");
                    System.out.println("Using " + next);
                }
            }
        }

        // return execGraph;
        return new ExecNodeGraph(execGraph.getFlinkVersion(), roots);
    }

    public void replaceInnerEdge(ExecNode<?> root, ExecNode<?> targetSource, ExecEdge newEdge) {
        int possibleIndex = findEdge(root.getInputEdges(), targetSource);
        if (possibleIndex >= 0) {
            System.out.println(
                    "Replace inner edge "
                            + root.getInputEdges().get(possibleIndex)
                            + " to "
                            + newEdge);
            root.replaceInputEdge(possibleIndex, newEdge);
            return;
        }

        // lets continue the search
        root.getInputEdges().stream()
                .map(ExecEdge::getSource)
                .forEach(node -> replaceInnerEdge(node, targetSource, newEdge));
    }

    public int findEdge(List<ExecEdge> edges, ExecNode<?> targetSource) {
        for (int i = 0; i < edges.size(); ++i) {
            if (edges.get(i).getSource().equals(targetSource)) {
                return i;
            }
        }

        return -1;
    }
}
