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
package org.apache.flink.table.planner.plan.nodes.physical.batch

import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, ExecNodeGraphGenerator, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.{BatchExecDynamicPartitionSink, BatchExecTableSourceScan}
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, RelExplainUtil}
import org.apache.flink.table.planner.utils.JavaScalaConversionUtil
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery

import java.util

/**
 * Batch physical RelNode to read data from an external source defined by a bounded
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
 */
class BatchPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: util.List[RelHint],
    tableSourceTable: TableSourceTable,
    val dppSink: BatchPhysicalDynamicPartitionSink = null)
  extends CommonPhysicalTableSourceScan(cluster, traitSet, hints, tableSourceTable)
  with BatchPhysicalRel {

  def copy(
      traitSet: RelTraitSet,
      tableSourceTable: TableSourceTable): BatchPhysicalTableSourceScan = {
    new BatchPhysicalTableSourceScan(cluster, traitSet, getHints, tableSourceTable, dppSink)
  }

  def copy(
      newTableSourceTable: TableSourceTable,
      dppSink: BatchPhysicalDynamicPartitionSink): BatchPhysicalTableSourceScan = {
    new BatchPhysicalTableSourceScan(cluster, traitSet, getHints, newTableSourceTable, dppSink)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val partitionFields = if (dppSink != null) {
      dppSink.partitionFields.map(i => dppSink.getRowType.getFieldNames.get(i)).mkString(",")
    } else {
      ""
    }
    super.explainTerms(pw).itemIf("dpp", partitionFields, dppSink != null)
  }

  override def computeSelfCost(planner: RelOptPlanner, mq: RelMetadataQuery): RelOptCost = {
    val rowCnt = mq.getRowCount(this)
    if (rowCnt == null) {
      return null
    }
    val cpu = 0
    val rowSize = mq.getAverageRowSize(this)
    val size = rowCnt * rowSize
    planner.getCostFactory.makeCost(rowCnt, cpu, size)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val tableSourceSpec = new DynamicTableSourceSpec(
      tableSourceTable.contextResolvedTable,
      util.Arrays.asList(tableSourceTable.abilitySpecs: _*))
    tableSourceSpec.setTableSource(tableSourceTable.tableSource)

    val ddpNode = if (dppSink != null) {
      val ddpNode = new ExecNodeGraphGenerator().generate(dppSink)
      ddpNode.asInstanceOf[BatchExecDynamicPartitionSink]
    } else {
      null
    }

    new BatchExecTableSourceScan(
      ddpNode,
      unwrapTableConfig(this),
      tableSourceSpec,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription)
  }
}
