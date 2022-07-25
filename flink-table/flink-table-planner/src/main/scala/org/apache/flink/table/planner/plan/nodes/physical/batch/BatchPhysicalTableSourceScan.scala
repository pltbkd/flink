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

import org.apache.calcite.plan._
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.hint.RelHint
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.util.mapping.IntPair
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.nodes.exec.ExecNode
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec
import org.apache.flink.table.planner.plan.nodes.physical.common.CommonPhysicalTableSourceScan
import org.apache.flink.table.planner.plan.rules.physical.batch.RuntimeFilterRule.BatchPhysicalRuntimeFilterBuilder
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import java.util
import java.util.Collections

/**
 * Batch physical RelNode to read data from an external source defined by a bounded
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
 */
class BatchPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: util.List[RelHint],
    tableSourceTable: TableSourceTable,
    var dppSink: RelNode = null,
    var df: RelNode = null,
    var dfUUID: String = null,
    var joinKeys: java.util.List[IntPair] = null)
  extends CommonPhysicalTableSourceScan(cluster, traitSet, hints, tableSourceTable)
  with BatchPhysicalRel {

  def copy(
      traitSet: RelTraitSet,
      tableSourceTable: TableSourceTable): BatchPhysicalTableSourceScan = {
    new BatchPhysicalTableSourceScan(cluster, traitSet, getHints, tableSourceTable, dppSink, df)
  }

  def copy(
      newTableSourceTable: TableSourceTable,
      newDppSink: BatchPhysicalDynamicPartitionSink): BatchPhysicalTableSourceScan = {
    new BatchPhysicalTableSourceScan(
      cluster,
      traitSet,
      getHints,
      newTableSourceTable,
      newDppSink,
      df,
      dfUUID,
      joinKeys)
  }

  def copy(
      newTableSourceTable: TableSourceTable,
      newDf: BatchPhysicalRuntimeFilterBuilder,
      dfUUID: String,
      joinKeys: java.util.List[IntPair]): BatchPhysicalTableSourceScan = {
    new BatchPhysicalTableSourceScan(
      cluster,
      traitSet,
      getHints,
      newTableSourceTable,
      dppSink,
      newDf,
      dfUUID,
      joinKeys)
  }

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    val input = if (inputs.size() == 0) null else inputs.get(0)
    val inputType =
      if (inputs.size() == 0) -1
      else {
        inputs.get(0) match {
          case vertex: HepRelVertex =>
            vertex.getCurrentRel match {
              case _: BatchPhysicalDynamicPartitionSink => 0
              case _: BatchPhysicalRuntimeFilterBuilder => 1
              case _ => -1
            }
          case _: BatchPhysicalDynamicPartitionSink => 0
          case _: BatchPhysicalRuntimeFilterBuilder => 1
          case _ => -1
        }
      }
    new BatchPhysicalTableSourceScan(
      cluster,
      traitSet,
      getHints,
      tableSourceTable,
      if (inputType == 0) input else null,
      if (inputType == 1) input else null,
      dfUUID,
      joinKeys)
  }

  override def replaceInput(ordinalInParent: Int, p: RelNode): Unit = {
    if (ordinalInParent == 0) {
      p match {
        case p: BatchPhysicalDynamicPartitionSink =>
          dppSink = p
        case p: BatchPhysicalRuntimeFilterBuilder =>
          df = p;
          assert(dfUUID != null)
          assert(joinKeys != null)
      }
    }
  }

  override def getInputs: util.List[RelNode] = {
    val input = getInput(0)
    if (input == null) {
      Collections.emptyList()
    } else {
      Collections.singletonList(input)
    }
  }

  override def getInput(i: Int): RelNode = {
    if (i != 0) {
      null
    } else if (dppSink != null) {
      dppSink
    } else if (df != null) {
      df
    } else {
      null
    }
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    pw.itemIf("dpp", dppSink, dppSink != null)
    pw.itemIf("df", df, df != null)
    super.explainTerms(pw)
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

    new BatchExecTableSourceScan(
      unwrapTableConfig(this),
      tableSourceSpec,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription,
      dppSink != null,
      df != null,
      dfUUID,
      joinKeys)
  }
}
