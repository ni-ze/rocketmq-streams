package org.apache.rocketmq.streams.topology.real;
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

import org.apache.rocketmq.streams.running.Processor;
import org.apache.rocketmq.streams.state.StateStore;

import java.util.function.Supplier;

public class StatefulProcessorFactory<K, V> extends ProcessorFactory<V> {
    private StateStore<K, V> stateStore;

    public StatefulProcessorFactory(String name, Supplier<Processor<V>> supplier) {
        super(name, supplier);
    }

    public StateStore<K, V> getStateStore() {
        return stateStore;
    }

    public void setStateStore(StateStore<K, V> stateStore) {
        this.stateStore = stateStore;
    }

    //对于有状态节点，这里提供的processor也是需要包含状态的
    @Override
    public Processor<V> build() {
        return super.build();
    }
}
