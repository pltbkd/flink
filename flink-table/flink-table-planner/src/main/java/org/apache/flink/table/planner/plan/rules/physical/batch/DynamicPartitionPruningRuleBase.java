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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.abilities.SupportsDynamicPartitionPruning;
import org.apache.flink.table.planner.calcite.FlinkTypeFactory;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalCalc;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalDynamicPartitionSink;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalJoinBase;
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchPhysicalTableSourceScan;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.planner.plan.utils.FlinkRexUtil;
import org.apache.flink.table.planner.utils.ShortcutUtils;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.hep.HepRelVertex;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.rel.core.JoinInfo;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.ImmutableIntList;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.configuration.ConfigOptions.key;

/** DynamicPartitionPruningRuleBase. */
public abstract class DynamicPartitionPruningRuleBase extends RelRule<RelRule.Config> {

    public static final ConfigOption<Boolean> TABLE_OPTIMIZER_DPP_ENABLED =
            key("table.optimizer.dpp-enabled").booleanType().defaultValue(true).withDescription("");

    public DynamicPartitionPruningRuleBase(RelRule.Config config) {
        super(config);
    }

    public boolean doMatches(
            BatchPhysicalJoinBase join,
            BatchPhysicalCalc calc,
            BatchPhysicalTableSourceScan factScan,
            boolean factInLeft) {
        if (!ShortcutUtils.unwrapContext(join).getTableConfig().get(TABLE_OPTIMIZER_DPP_ENABLED)) {
            return false;
        }
        if (factScan.dppSink() != null) {
            // rule applied
            return false;
        }

        // TODO support more join types
        if (join.getJoinType() != JoinRelType.INNER) {
            return false;
        }

        // TODO Only support FLIP-27 source
        if (calc.getProgram().getCondition() == null) {
            return false;
        }

        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        if (tableSourceTable == null) {
            return false;
        }
        if (!(tableSourceTable.tableSource() instanceof SupportsDynamicPartitionPruning)) {
            return false;
        }
        CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getTable();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        if (partitionKeys.isEmpty()) {
            return false;
        }

        ImmutableIntList partitionList =
                ImmutableIntList.of(
                        partitionKeys.stream()
                                .map(i -> factScan.getRowType().getFieldNames().indexOf(i))
                                .mapToInt(i -> i)
                                .toArray());
        JoinInfo joinInfo = join.analyzeCondition();
        if (factInLeft) {
            return joinInfo.leftKeys.containsAll(partitionList);
        } else {
            return joinInfo.rightKeys.containsAll(partitionList);
        }
    }

    protected BatchPhysicalTableSourceScan createNewTableSourceScan(
            BatchPhysicalTableSourceScan factScan,
            RelNode dimSide,
            BatchPhysicalJoinBase join,
            boolean leftIsFact) {
        final Pair<int[], int[]> partitionFields =
                extractPartitionFieldsInDimSide(factScan, join, leftIsFact);
        int[] dimPartitionFields = partitionFields.left;
        int[] factPartitionFields = partitionFields.right;

        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        DynamicTableSource newTableSource = tableSourceTable.tableSource().copy();

        List<String> dynamicPartitionKeys =
                Arrays.stream(factPartitionFields)
                        .mapToObj(i -> factScan.getRowType().getFieldNames().get(i))
                        .collect(Collectors.toList());
        ((SupportsDynamicPartitionPruning) newTableSource)
                .applyDynamicPartitionPruning(dynamicPartitionKeys);

        TableSourceTable newTable =
                tableSourceTable.copy(
                        newTableSource,
                        tableSourceTable.getRowType(),
                        tableSourceTable.abilitySpecs());

        final BatchPhysicalDynamicPartitionSink dppSink =
                createDynamicPartitionSink(dimSide, dimPartitionFields);

        return factScan.copy(newTable, dppSink);
    }

    private BatchPhysicalDynamicPartitionSink createDynamicPartitionSink(
            RelNode dimSide, int[] dimPartitionFields) {
        final RelDataType outputType =
                ((FlinkTypeFactory) dimSide.getCluster().getTypeFactory())
                        .projectStructType(dimSide.getRowType(), dimPartitionFields);
        RelNode input = createDynamicPartitionSinkInput(dimSide);

        return new BatchPhysicalDynamicPartitionSink(
                dimSide.getCluster(), dimSide.getTraitSet(), input, outputType, dimPartitionFields);
    }

    private RelNode createDynamicPartitionSinkInput(RelNode dimSide) {
        RelShuttle relShuttle =
                new DefaultRelShuttle() {
                    @Override
                    public RelNode visit(RelNode rel) {
                        if (rel instanceof HepRelVertex) {
                            return super.visit(((HepRelVertex) rel).getCurrentRel());
                        } else {
                            return super.visit(rel);
                        }
                    }
                };
        return dimSide.accept(relShuttle);
    }

    private Pair<int[], int[]> extractPartitionFieldsInDimSide(
            BatchPhysicalTableSourceScan factScan, BatchPhysicalJoinBase join, boolean leftIsFact) {
        TableSourceTable tableSourceTable = factScan.getTable().unwrap(TableSourceTable.class);
        CatalogTable catalogTable = tableSourceTable.contextResolvedTable().getTable();
        List<String> partitionKeys = catalogTable.getPartitionKeys();
        ImmutableIntList partitionList =
                ImmutableIntList.of(
                        partitionKeys.stream()
                                .map(i -> factScan.getRowType().getFieldNames().indexOf(i))
                                .mapToInt(i -> i)
                                .toArray());

        List<Integer> dimPartitionFields = new ArrayList<>();
        List<Integer> factPartitionFields = new ArrayList<>();
        int maxCnfNodeCount =
                ShortcutUtils.unwrapContext(factScan)
                        .getTableConfig()
                        .get(FlinkRexUtil.TABLE_OPTIMIZER_CNF_NODES_LIMIT());
        final RexNode cnf =
                FlinkRexUtil.toCnf(
                        factScan.getCluster().getRexBuilder(),
                        maxCnfNodeCount,
                        join.getCondition());
        final int leftFieldCount = join.getLeft().getRowType().getFieldCount();
        // converts the cnf condition to a list of AND conditions
        final List<RexNode> conditions = RelOptUtil.conjunctions(cnf);
        for (RexNode rexNode : conditions) {
            if (!(rexNode instanceof RexCall)) {
                continue;
            }
            RexCall rexCall = (RexCall) rexNode;
            if (rexCall.getOperator() != SqlStdOperatorTable.EQUALS) {
                continue;
            }
            RexNode operand0 = rexCall.getOperands().get(0);
            RexNode operand1 = rexCall.getOperands().get(1);
            if (!(operand0 instanceof RexInputRef) || !(operand1 instanceof RexInputRef)) {
                continue;
            }
            int index0 = ((RexInputRef) operand0).getIndex();
            int index1 = ((RexInputRef) operand1).getIndex();
            if (leftIsFact) {
                if (index0 < leftFieldCount
                        && index1 >= leftFieldCount
                        && partitionList.contains(index0)) {
                    factPartitionFields.add(index0);
                    dimPartitionFields.add(index1 - leftFieldCount);
                }
                if (index1 < leftFieldCount
                        && index0 >= leftFieldCount
                        && partitionList.contains(index1)) {
                    factPartitionFields.add(index1);
                    dimPartitionFields.add(index0 - leftFieldCount);
                }
            } else {
                if (index1 >= leftFieldCount
                        && index0 < leftFieldCount
                        && partitionList.contains(index1 - leftFieldCount)) {
                    factPartitionFields.add(index1 - leftFieldCount);
                    dimPartitionFields.add(index0);
                }
                if (index0 >= leftFieldCount
                        && index1 < leftFieldCount
                        && partitionList.contains(index0 - leftFieldCount)) {
                    factPartitionFields.add(index1 - leftFieldCount);
                    dimPartitionFields.add(index1);
                }
            }
        }
        return new Pair<>(
                dimPartitionFields.stream().mapToInt(i -> i).toArray(),
                factPartitionFields.stream().mapToInt(i -> i).toArray());
    }
}
