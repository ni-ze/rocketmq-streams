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
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class SinkSupplier<T> implements Supplier<Processor<T>> {
    private String topicName;

    public SinkSupplier(String topicName) {
        this.topicName = topicName;
    }

    @Override
    public Processor<T> get() {
        return new SinkProcessor();
    }

    private class SinkProcessor extends AbstractProcessor<T> {
        private StreamContext<T> context;

        @Override
        public void preProcess(StreamContext<T> context) {
            this.context = context;
            this.context.init(null);
        }

        @Override
        public void process(T data) {
            if (data != null) {
                Context<Object, T> result = new Context<>(this.context.getKey(), data);
                result.setSinkTopic(topicName);
                this.context.forward(result);
            }

        }
    }
}
