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

package org.apache.flink.connector.file.sink.compactor;

import org.apache.flink.api.connector.sink2.CommittableMessage;

import java.io.Serializable;
import java.util.List;

public interface CompactRequestPacker<CommT, CompT> extends Serializable {
    /** Add a new committable to packing bins. */
    void accept(CommittableMessage<CommT> message);

    /**
     * Notify that no more committable will be added so remaining committable should be packed
     * immediately.
     */
    void endInput();

    /** Returns remaining committable should be stored in snapshot. */
    List<CommittableMessage<CommT>> getRemaining();

    /**
     * Returns an available packed request, the method will be called until returning null
     * indicating requests are exhausted.
     */
    CompT getPackedRequest();

    //    public static class SizeThreshold<CommT> extends CompactRequestPacker<CommT> {
    //        private final long threshold;
    //        private final Function<CommT, Long> sizeRetriever;
    //
    //        private List<CommittableMessage<CommT>> packing = new ArrayList<>();
    //        private long size = 0;
    //
    //        private final Queue<CompactRequest<CommT>> requests = new LinkedList<>();
    //
    //        public SizeThreshold(long threshold, Function<CommT, Long> sizeRetriever) {
    //            this.threshold = threshold;
    //            this.sizeRetriever = sizeRetriever;
    //        }
    //
    //        @Override
    //        public void accept(CommittableMessage<CommT> message) {
    //            if (message instanceof CommittableWithLineage) {
    //                long curSize =
    //                        sizeRetriever.apply(
    //                                ((CommittableWithLineage<CommT>) message).getCommittable());
    //                if (size + curSize > threshold) {
    //                    requests.add(new CompactRequest<>(packing));
    //                    packing = new ArrayList<>();
    //                    size = curSize;
    //                } else {
    //                    packing.add(message);
    //                    size += curSize;
    //                }
    //            }
    //        }
    //
    //        @Override
    //        public List<CommittableMessage<CommT>> getRemaining() {
    //            assert requests.isEmpty();
    //            return packing;
    //        }
    //
    //        @Override
    //        public boolean hasFilledRequest() {
    //            return !requests.isEmpty();
    //        }
    //
    //        @Override
    //        public CompactRequest<CommT> getFilledRequest() {
    //            return requests.poll();
    //        }
    //
    //        @Override
    //        public void endInput() {
    //            requests.add(new CompactRequest<>(packing));
    //            packing = new ArrayList<>();
    //        }
    //    }
}
