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

import org.apache.flink.table.planner.plan.nodes.exec.ExecNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeGraph;
import org.apache.flink.table.planner.plan.nodes.exec.visitor.AbstractExecNodeExactlyOnceVisitor;

import org.apache.commons.lang3.StringUtils;

public class OutputGraphProcessor implements ExecNodeGraphProcessor {

    @Override
    public ExecNodeGraph process(ExecNodeGraph execGraph, ProcessorContext context) {
        AbstractExecNodeExactlyOnceVisitor dppScanCollector =
                new AbstractExecNodeExactlyOnceVisitor() {
                    int indent = 0;

                    @Override
                    protected void visitNode(ExecNode<?> node) {
                        System.out.println(StringUtils.repeat("  ", indent) + "|-" + node.getDescription());
                        indent += 4;
                        visitInputs(node);
                        indent -= 4;
                    }
                };
        execGraph.getRootNodes().forEach(node -> node.accept(dppScanCollector));

        return execGraph;
    }
}
