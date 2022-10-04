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
import org.apache.rocketmq.streams.function.FilterAction;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class FilterActionSupplier<K, V> implements Supplier<Processor<K, V, K, V>> {
    private FilterAction<K, V> filterAction;

    public FilterActionSupplier(FilterAction<K, V> filterAction) {
        this.filterAction = filterAction;
    }

    @Override
    public Processor<K, V, K, V> get() {
        return new FilterProcessor(filterAction);
    }

    private class FilterProcessor extends AbstractProcessor<K, V, K, V> {
        private final FilterAction<K, V> filterAction;
        private StreamContext<K, V, K, V>  context;

        public FilterProcessor(FilterAction<K, V> filterAction) {
            this.filterAction = filterAction;
        }


        @Override
        public void preProcess(StreamContext<K, V, K, V>  context) {
            this.context = context;
            this.context.init(super.getChildren());
        }

        @Override
        public  void process(Context<K, V> context) {
            boolean pass = filterAction.apply(context.getKey(), context.getValue());
            if (pass) {
                this.context.forward(context);
            }
        }
    }
}
