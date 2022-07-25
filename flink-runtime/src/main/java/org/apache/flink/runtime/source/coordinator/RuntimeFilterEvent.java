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

import org.apache.flink.api.connector.source.SourceEvent;

/** RuntimeFilterEvent */
public class RuntimeFilterEvent implements SourceEvent {
    private final String uuid;
    private final int estimatedBuildCount;
    private final int bloomFilterByteSize;
    private final byte[] filterBits;

    public RuntimeFilterEvent(
            String uuid, int estimatedBuildCount, int bloomFilterByteSize, byte[] filterBits) {
        this.uuid = uuid;
        this.estimatedBuildCount = estimatedBuildCount;
        this.bloomFilterByteSize = bloomFilterByteSize;
        this.filterBits = filterBits;
    }

    public String getUuid() {
        return uuid;
    }

    public int getEstimatedBuildCount() {
        return estimatedBuildCount;
    }

    public int getBloomFilterByteSize() {
        return bloomFilterByteSize;
    }

    public byte[] getFilterBits() {
        return filterBits;
    }

    @Override
    public String toString() {
        return "RuntimeFilterEvent{uuid=" + uuid + "}";
    }
}
