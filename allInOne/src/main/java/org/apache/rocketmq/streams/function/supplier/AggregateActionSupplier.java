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
import org.apache.rocketmq.streams.metadata.Context;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;
import org.apache.rocketmq.streams.state.StateStore;

import java.util.function.Supplier;

public class AggregateActionSupplier<K, V, OV> implements Supplier<Processor<V>> {
    private final String currentName;
    private final String parentName;
    private StateStore<K, OV> stateStore;
    private Supplier<OV> initAction;
    private AggregateAction<K, V, OV> aggregateAction;

    public AggregateActionSupplier(String currentName, String parentName,
                                   StateStore<K, OV> stateStore, Supplier<OV> initAction,
                                   AggregateAction<K, V, OV> aggregateAction) {
        this.currentName = currentName;
        this.parentName = parentName;
        this.stateStore = stateStore;
        this.initAction = initAction;
        this.aggregateAction = aggregateAction;
    }

    @Override
    public Processor<V> get() {
        return new AggregateProcessor(currentName, parentName, stateStore, initAction, aggregateAction);
    }

    private class AggregateProcessor extends AbstractProcessor<V> {
        private final String currentName;
        private final String parentName;
        private final Supplier<OV> initAction;
        private final StateStore<K, OV> stateStore;
        private final AggregateAction<K, V, OV> aggregateAction;
        private StreamContext<V> context;

        public AggregateProcessor(String currentName, String parentName,
                                  StateStore<K, OV> stateStore, Supplier<OV> initAction,
                                  AggregateAction<K, V, OV> aggregateAction) {
            this.currentName = currentName;
            this.parentName = parentName;
            this.initAction = initAction;
            this.stateStore = stateStore;
            this.aggregateAction = aggregateAction;

            this.stateStore.init();
        }

        @Override
        public void preProcess(StreamContext<V> context) {
            this.context = context;
            this.context.init(super.getChildren());
        }

        @Override
        public void process(V data) {
            K key = this.context.getKey();
            OV value = stateStore.get(key);
            if (value == null) {
                value = initAction.get();
            }

            OV result = aggregateAction.calculate(key, data, value);

            stateStore.put(key, result);

            Context<K, V> convert = super.convert(new Context<>(key, result));

            this.context.forward(convert);
        }

        //1
//        @Override
//        public void process(Context<K, V> context) {
//            K key = context.getKey();
//            OV value = stateStore.get(key);
//            if (value == null) {
//                value = initAction.get();
//            }
//            OV out = aggregateAction.calculate(key, context.getValue(), value);
//
//            stateStore.put(key, out);
//
//            Context<K, V> result = super.convert(context.value(out));
//
//            this.context.forward(result);
//        }
    }
}
