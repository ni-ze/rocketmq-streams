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

package org.apache.rocketmq.streams.examples.aggregate;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.lang.StringUtils;
import org.apache.rocketmq.streams.script.service.IAccumulator;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class TestUDAF implements IAccumulator<List<String>, TestUDAF.Trace> {
    public static class Trace {
        public Map<String, JSONObject> spanId2Span = new HashMap<>();

        public Map<String, String> spanId2Parent = new HashMap<>();

        public Map<String, List<String>> parent2Children = new HashMap<>();

        public List<String> result = new ArrayList<>();
    }

    @Override
    public Trace createAccumulator() {
        return new Trace();
    }

    @Override
    public List<String> getValue(Trace accumulator) {
        return accumulator.result;
    }

    @Override
    public void accumulate(Trace accumulator, Object... parameters) {
        if (parameters == null || parameters.length == 0) {
            return;
        }
        if (parameters.length != 1) {
            throw new IllegalArgumentException("parameters length must be one");
        }

        JSONObject param = (JSONObject) parameters[0];
        String result = param.toJSONString();

        if (accumulator == null) {
            accumulator = new Trace();
        }

        accumulator.result.add(result);
    }

    @Override
    public void merge(Trace accumulator, Iterable<Trace> its) {

    }

    @Override
    public void retract(Trace accumulator, String... parameters) {

    }


}
