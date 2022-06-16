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

package org.apache.flink.table.planner.plan.rules.physical.batch;

import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalExchange;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;

import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Join;

import java.util.Arrays;

/** DynamicPartitionPruningRule2. */
public class DynamicPartitionPruningRule2 extends DynamicPartitionPruningRuleBase {

    public static final RelOptRule FACT_IN_LEFT =
            DynamicPartitionPruningRule2.Config.EMPTY
                    .withDescription("DynamicPartitionPruningRule2:factInLeft")
                    .as(Config.class)
                    .factInLeft()
                    .toRule();

    public DynamicPartitionPruningRule2(RelRule.Config config) {
        super(config);
    }

    /** Config. */
    public interface Config extends RelRule.Config {
        @Override
        default DynamicPartitionPruningRule2 toRule() {
            return new DynamicPartitionPruningRule2(this);
        }

        default Config factInLeft() {
            return withOperandSupplier(
                            b0 ->
                                    b0.operand(BatchPhysicalJoinBase.class)
                                            .inputs(
                                                    l ->
                                                            l.operand(
                                                                            BatchPhysicalTableSourceScan
                                                                                    .class)
                                                                    .noInputs(),
                                                    r ->
                                                            r.operand(BatchPhysicalExchange.class)
                                                                    .oneInput(
                                                                            e ->
                                                                                    e.operand(
                                                                                                    BatchPhysicalCalc
                                                                                                            .class)
                                                                                            .oneInput(
                                                                                                    c ->
                                                                                                            c.operand(
                                                                                                                            BatchPhysicalTableSourceScan
                                                                                                                                    .class)
                                                                                                                    .noInputs()))))
                    .as(Config.class);
        }
    }

    @Override
    public boolean matches(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
        final BatchPhysicalCalc calc = call.rel(3);
        final BatchPhysicalTableSourceScan factScan = call.rel(1);
        return doMatches(join, calc, factScan, true);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        final BatchPhysicalJoinBase join = call.rel(0);
        final BatchPhysicalTableSourceScan factScan = call.rel(1);
        final RelNode dimSide = call.rel(2);

        final BatchPhysicalTableSourceScan newFactScan =
                createNewTableSourceScan(factScan, dimSide.getInput(0), join, true);
        final Join newJoin = join.copy(join.getTraitSet(), Arrays.asList(newFactScan, dimSide));
        call.transformTo(newJoin);
    }
}
