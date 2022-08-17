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
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, InputProperty}
import org.apache.flink.table.planner.plan.nodes.exec.batch.BatchExecTableSourceScan
import org.apache.flink.table.planner.plan.nodes.exec.spec.DynamicTableSourceSpec
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.planner.utils.ShortcutUtils.unwrapTableConfig

import com.google.common.collect.ImmutableList
import org.apache.calcite.plan._
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rel.hint.RelHint

import java.util
import java.util.UUID

/**
 * Batch physical RelNode to read data from an external source defined by a bounded
 * [[org.apache.flink.table.connector.source.ScanTableSource]].
 */
class BatchPhysicalDynamicFilteringTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    hints: util.List[RelHint],
    tableSourceTable: TableSourceTable,
    inputs: util.List[RelNode],
    val uuid: String
) extends BatchPhysicalTableSourceScan(cluster, traitSet, hints, tableSourceTable) {

  def this(
      cluster: RelOptCluster,
      traitSet: RelTraitSet,
      hints: util.List[RelHint],
      tableSourceTable: TableSourceTable,
      inputs: util.List[RelNode]) =
    this(cluster, traitSet, hints, tableSourceTable, inputs, UUID.randomUUID.toString)

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new BatchPhysicalDynamicFilteringTableSourceScan(
      cluster,
      traitSet,
      getHints,
      tableSourceTable,
      inputs,
      uuid)
  }

  override def copy(
      traitSet: RelTraitSet,
      tableSourceTable: TableSourceTable): BatchPhysicalTableSourceScan = {
    new BatchPhysicalDynamicFilteringTableSourceScan(
      cluster,
      traitSet,
      getHints,
      tableSourceTable,
      inputs,
      uuid)
  }

  override def replaceInput(ordinalInParent: Int, rel: RelNode): Unit = {
    this.inputs.set(ordinalInParent, rel)
    recomputeDigest()
  }

  override def getInputs: util.List[RelNode] = {
    ImmutableList.copyOf(inputs.toArray(Array[RelNode]()))
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    for (i <- 0 until inputs.size()) {
      pw.input("input#" + i, inputs.get(i))
    }
    super.explainTerms(pw).item("uuid", uuid)
  }

  override def translateToExecNode(): ExecNode[_] = {
    val tableSourceSpec = new DynamicTableSourceSpec(
      tableSourceTable.contextResolvedTable,
      util.Arrays.asList(tableSourceTable.abilitySpecs: _*))
    tableSourceSpec.setTableSource(tableSourceTable.tableSource)
    val inputProperties = new util.ArrayList[InputProperty]
    for (_ <- inputs.toArray) {
      inputProperties.add(InputProperty.DEFAULT)
    }

    new BatchExecTableSourceScan(
      unwrapTableConfig(this),
      tableSourceSpec,
      inputProperties,
      FlinkTypeFactory.toLogicalRowType(getRowType),
      getRelDetailedDescription,
      uuid)
  }
}
