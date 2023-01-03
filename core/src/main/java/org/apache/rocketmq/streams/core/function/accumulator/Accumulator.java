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
package org.apache.rocketmq.streams.core.function.accumulator;

import java.util.Properties;

//因为需要序列化/反序列化这个类，所以必须给field生成setter/getter方法
public interface Accumulator<V, R> {
    void addValue(V value);

    void merge(Accumulator<V, R> other);

    /**
     * 状态触发后调用，context可以加入状态触发后产生的某些条件，传递给算子
     * @param context
     * @return
     */
    R result(Properties context);

    Accumulator<V, R> clone();
}