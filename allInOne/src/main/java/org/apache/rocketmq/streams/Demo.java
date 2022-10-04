package org.apache.rocketmq.streams;
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

import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.streams.common.Constant;
import org.apache.rocketmq.streams.rstream.RStream;
import org.apache.rocketmq.streams.rstream.StreamBuilder;
import org.apache.rocketmq.streams.serialization.Wrapper;
import org.apache.rocketmq.streams.topology.TopologyBuilder;

import java.util.Properties;

public class Demo {
    public static void main(String[] args) {
        StreamBuilder builder = new StreamBuilder();

        RStream<String, String> rStream = builder.source("sourceTopic");

        rStream.map((key, value) -> value)
                .filter((key, value) -> value != null)
                .print();

        TopologyBuilder topologyBuilder = builder.build();

        Properties properties = new Properties();
        properties.putIfAbsent(MixAll.NAMESRV_ADDR_PROPERTY, "127.0.0.1:9876");
        properties.putIfAbsent(Constant.DEFAULT_SERDE_CLASS_CONFIG, Wrapper.StringSerde.class.getName());

        RocketMQStream rocketMQStream = new RocketMQStream(topologyBuilder, properties);

        rocketMQStream.start();
    }

}
