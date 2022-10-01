package org.apache.rocketmq.streams.function.supplier;
/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import org.apache.rocketmq.streams.function.AggregateAction;
import org.apache.rocketmq.streams.metadata.Data;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;
import org.apache.rocketmq.streams.state.StateStore;

import java.util.function.Supplier;

public class AggregateActionSupplier<K,T,OUT> implements Supplier<Processor<T>> {
    private final String currentName;
    private final String parentName;

    private Supplier<OUT> initAction;
    private AggregateAction<K, T, OUT> aggregateAction;

    public AggregateActionSupplier(String currentName, String parentName, Supplier<OUT> initAction,
                                   AggregateAction<K, T, OUT> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;

        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<T> get() {
        return new AggregateProcessor(currentName, parentName, initAction, aggregateAction);
    }

    private class AggregateProcessor extends AbstractProcessor<T> {
        private final String currentName;
        private final String parentName;
        private final Supplier<OUT> initAction;

        private final AggregateAction<K, T, OUT> aggregateAction;
        private StreamContext context;

        public AggregateProcessor(String currentName, String parentName, Supplier<OUT> initAction,
                                  AggregateAction<K, T, OUT> aggregateAction) {
            this.currentName = currentName;
            this.parentName = parentName;
            this.initAction = initAction;

            this.aggregateAction = aggregateAction;
        }

        @Override
        public void preProcess(StreamContext context) {
            this.context = context;
            this.context.init(super.getChildren());
        }



        @Override
        public void process(T data) {
            K key = data.getKey();
            OV value = stateStore.get(key);
            if (value == null) {
                value = initAction.get();
            }
            OV out = aggregateAction.calculate(key, data.getValue(), value);

            stateStore.put(key, out);

            Data<K, OV> result = data.value(out);

            context.forward(result);
        }
    }
}
