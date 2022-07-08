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

package org.apache.flink.table.runtime.operators.dpp;

import org.apache.flink.runtime.operators.coordination.OperatorEventGateway;
import org.apache.flink.runtime.source.event.SourceEventWrapper;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.connector.source.DynamicPartitionEvent;
import org.apache.flink.table.connector.source.PartitionData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.Row;

import java.util.ArrayList;
import java.util.List;

/** This is a dynamic partition operator. */
public class DynamicPartitionOperator extends AbstractStreamOperator<Object>
        implements OneInputStreamOperator<RowData, Object> {

    private final RowType partitionFieldType;
    private final List<Integer> partitionFieldIndices;
    private transient List<Row> buffer;

    /** The event gateway through which this operator talks to its coordinator. */
    private transient OperatorEventGateway operatorEventGateway;

    public DynamicPartitionOperator(
            RowType partitionFieldType, List<Integer> partitionFieldIndices) {
        this.partitionFieldType = partitionFieldType;
        this.partitionFieldIndices = partitionFieldIndices;
    }

    @Override
    public void open() throws Exception {
        super.open();
        this.buffer = new ArrayList<>();
    }

    public void setOperatorEventGateway(OperatorEventGateway operatorEventGateway) {
        this.operatorEventGateway = operatorEventGateway;
    }

    @Override
    public void processElement(StreamRecord<RowData> element) throws Exception {
        // TODO convert to the specific type
        Object[] values = new Object[partitionFieldIndices.size()];
        for (int i = 0; i < partitionFieldIndices.size(); ++i) {
            LogicalType type = partitionFieldType.getTypeAt(i);
            int index = partitionFieldIndices.get(i);
            switch (type.getTypeRoot()) {
                case INTEGER:
                    values[i] = "" + element.getValue().getInt(index);
                    break;
                case BIGINT:
                    values[i] = "" + element.getValue().getLong(index);
                    break;
                case VARCHAR:
                    values[i] = element.getValue().getString(index).toString();
                    break;
                default:
                    throw new UnsupportedOperationException();
            }
        }
        buffer.add(Row.of(values));
    }

    public void finish() throws Exception {
        DynamicPartitionEvent event = new DynamicPartitionEvent(new PartitionData(buffer));
        operatorEventGateway.sendEventToCoordinator(new SourceEventWrapper(event));
    }

    @Override
    public void close() throws Exception {
        super.close();
        if (buffer != null) {
            buffer.clear();
        }
    }
}
