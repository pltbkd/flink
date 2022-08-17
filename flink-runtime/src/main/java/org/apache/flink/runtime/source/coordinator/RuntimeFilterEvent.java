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

package org.apache.flink.runtime.source.coordinator;

import org.apache.flink.api.connector.source.RuntimeFilter;
import org.apache.flink.api.connector.source.SourceEvent;

/** RuntimeFilterEvent */
public class RuntimeFilterEvent<T> implements SourceEvent {
    private final String receiverUUID;

    private final ReceiverType receiverType;

    private final RuntimeFilter<T> filter;

    public RuntimeFilterEvent(
            String receiverUUID, ReceiverType receiverType, RuntimeFilter<T> filter) {
        this.receiverUUID = receiverUUID;
        this.receiverType = receiverType;
        this.filter = filter;
    }

    public String getReceiverUUID() {
        return receiverUUID;
    }

    public ReceiverType getReceiverType() {
        return receiverType;
    }

    public RuntimeFilter<T> getFilter() {
        return filter;
    }

    public enum ReceiverType {
        ENUMERATOR,
        OPERATOR
    }

    @Override
    public String toString() {
        return "RuntimeFilterEvent{uuid=" + receiverUUID + ", receiverType=" + receiverType + "}";
    }
}
