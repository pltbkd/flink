/*
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package org.apache.flink.streaming.api.transformations;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.sink2.Sink;
import org.apache.flink.api.connector.sink2.SinkWriter;
import org.apache.flink.api.dag.Transformation;
import org.apache.flink.streaming.api.operators.ChainingStrategy;

import org.apache.flink.shaded.guava30.com.google.common.collect.Lists;

import java.util.Collections;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * A {@link Transformation} for {@link Sink}.
 *
 * @param <InputT> The input type of the {@link SinkWriter}
 * @param <OutputT> The output type of the {@link Sink}
 */
@Internal
public class SinkTransformation<InputT, OutputT> extends PhysicalTransformation<OutputT> {

    private final Transformation<InputT> input;

    private ChainingStrategy chainingStrategy;

    public SinkTransformation(
            Transformation<InputT> input,
            TypeInformation<OutputT> outputType,
            String name,
            int parallelism) {
        super(name, outputType, parallelism);
        this.input = checkNotNull(input);
    }

    @Override
    public void setChainingStrategy(ChainingStrategy strategy) {
        chainingStrategy = checkNotNull(strategy);
    }

    @Override
    public List<Transformation<?>> getTransitivePredecessors() {
        return Lists.newArrayList();
    }

    @Override
    public List<Transformation<?>> getInputs() {
        return Collections.singletonList(input);
    }

    public ChainingStrategy getChainingStrategy() {
        return chainingStrategy;
    }
}
