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

package org.apache.flink.table.runtime.operators.dynamicfiltering;

import org.apache.flink.streaming.api.operators.AbstractStreamOperatorFactory;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.StreamOperator;
import org.apache.flink.streaming.api.operators.StreamOperatorParameters;

/**
 * The factory class for {@link ExecutionOrderEnforcerOperator}. This is a simple operator factory
 * whose chaining strategy is always ChainingStrategy.HEAD_WITH_SOURCES.
 */
public class ExecutionOrderEnforcerOperatorFactory<IN> extends AbstractStreamOperatorFactory<IN> {

    private final int numberOfInputs;

    public ExecutionOrderEnforcerOperatorFactory(int numberOfInputs) {
        this.numberOfInputs = numberOfInputs;
    }

    @Override
    public <T extends StreamOperator<IN>> T createStreamOperator(
            StreamOperatorParameters<IN> parameters) {
        return (T) new ExecutionOrderEnforcerOperator<>(parameters, numberOfInputs);
    }

    @Override
    public ChainingStrategy getChainingStrategy() {
        return ChainingStrategy.HEAD_WITH_SOURCES;
    }

    @Override
    public Class<? extends StreamOperator> getStreamOperatorClass(ClassLoader classLoader) {
        return ExecutionOrderEnforcerOperator.class;
    }
}
