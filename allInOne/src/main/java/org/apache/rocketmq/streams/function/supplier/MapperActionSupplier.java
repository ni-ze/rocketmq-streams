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

import org.apache.rocketmq.streams.function.KeySelectAction;
import org.apache.rocketmq.streams.metadata.Data;
import org.apache.rocketmq.streams.running.AbstractProcessor;
import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.running.StreamContext;

import java.util.function.Supplier;

public class MapperActionSupplier<K, V, NK> implements Supplier<Processor<K, V, NK, V>> {
    private final KeySelectAction<K, V, NK> keySelectAction;

    public MapperActionSupplier(KeySelectAction<K, V, NK> keySelectAction) {
        this.keySelectAction = keySelectAction;
    }

    @Override
    public Processor<K, V, NK, V> get() {
        return new MapperProcessor(keySelectAction);
    }

    private class MapperProcessor extends AbstractProcessor<K, V, NK, V> {
        private final KeySelectAction<K, V, NK> keySelectAction;
        private StreamContext streamContext;

        public MapperProcessor(KeySelectAction<K, V, NK> keySelectAction) {
            this.keySelectAction = keySelectAction;
        }

        @Override
        public void preProcess(StreamContext context) {
            this.streamContext = context;
            this.streamContext.init(super.getChildren());
        }

        @Override
        public void process(Data<K, V> data) {
            NK newKey = keySelectAction.select(data.getKey(), data.getValue());
            Data<NK, V> newData = data.key(newKey);
            this.streamContext.forward(newData);
        }
    }
}
