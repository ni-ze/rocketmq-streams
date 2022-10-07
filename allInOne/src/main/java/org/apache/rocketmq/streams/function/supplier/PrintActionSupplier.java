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

import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class PrintActionSupplier<T> implements Supplier<Processor<T>> {


    @Override
    public Processor<T> get() {
        return new PrintProcessor<>();
    }

    static class PrintProcessor<T> extends AbstractProcessor<T> {
        private StreamContext<T> streamContext;

        public PrintProcessor() {
        }

        @Override
        public void preProcess(StreamContext<T> context) {
            this.streamContext = context;
            this.streamContext.init(super.getChildren());
        }

        @Override
        public void process(T data) {
            System.out.println("print sink: " + "key:[" + this.streamContext.getKey() + "]" + "value:[" + data + "]");
        }
    }

}
