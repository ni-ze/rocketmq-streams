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

import org.apache.rocketmq.streams.metadata.Context;
import org.apache.rocketmq.streams.function.ValueMapperAction;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class ValueActionSupplier<K, V, OV> implements Supplier<Processor<K, V, K, OV>> {
    private final ValueMapperAction<K, V, OV> valueMapperAction;


    public ValueActionSupplier(ValueMapperAction<K, V, OV> valueMapperAction) {
        this.valueMapperAction = valueMapperAction;
    }

    @Override
    public Processor<K, V, K, OV> get() {
        return new ValueMapperProcessor<>(this.valueMapperAction);
    }


    static class ValueMapperProcessor<K, V, OV> extends AbstractProcessor<K, V, K, OV> {
        private final ValueMapperAction<K,V, OV> valueMapperAction;
        private StreamContext<K, V, K, OV> context;

        public ValueMapperProcessor(ValueMapperAction<K, V, OV> valueMapperAction) {
            this.valueMapperAction = valueMapperAction;
        }

        @Override
        public void preProcess(StreamContext<K, V, K, OV> context) {
            this.context = context;
            this.context.init(super.getChildren());
        }

        @Override
        public void process(Context<K, V> context) {
            OV value = valueMapperAction.convert(context.getKey(), context.getValue());
            Context<K, V> result = super.convert(context.value(value));
            this.context.forward(result);
        }
    }

}
