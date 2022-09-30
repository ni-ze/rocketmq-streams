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

import org.apache.rocketmq.streams.metadata.Data;
import org.apache.rocketmq.streams.function.FilterAction;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class FilterActionSupplier<T> implements Supplier<Processor<T>> {
    private FilterAction<T> filterAction;

    public FilterActionSupplier(FilterAction<T> filterAction) {
        this.filterAction = filterAction;
    }

    @Override
    public Processor<T> get() {
        return new FilterProcessor(filterAction);
    }

    private class FilterProcessor extends AbstractProcessor<T> {
        private final FilterAction<T> filterAction;
        private StreamContext  context;

        public FilterProcessor(FilterAction<T> filterAction) {
            this.filterAction = filterAction;
        }


        @Override
        public void preProcess(StreamContext  context) {
            this.context = context;
            this.context.init(super.getChildren());
        }

        @Override
        public void process(Data<T> data) {
            boolean pass = filterAction.apply(data.getData());
            if (pass) {
                this.context.forward(data);
            }
        }
    }
}
