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
import org.apache.rocketmq.streams.client.StreamBuilder;
import org.apache.rocketmq.streams.client.source.DataStreamSource;
import org.apache.rocketmq.streams.client.strategy.WindowStrategy;
import org.apache.rocketmq.streams.client.transform.window.Time;
import org.apache.rocketmq.streams.client.transform.window.TumblingWindow;

import java.util.Set;

public class UserTest {
    public static void main(String[] args) {
        String temp = "{\n" +
                "    \"empty\":false,\n" +
                "    \"labels\":[\n" +
                "        \"68e51083e737a3f7d9c9e7ab34b1f6a7\"\n" +
                "    ],\n" +
                "    \"spanIndex\":{\n" +
                "        \"0.1.1\":{\n" +
                "            \"appId\":\"1_1@811c06548ae3a13\",\n" +
                "            \"clientAppId\":\"1_1@10b8d040e1d0f78\",\n" +
                "            \"clientIp\":\"11.239.127.90\",\n" +
                "            \"elapsed\":1,\n" +
                "            \"httpStatusCode\":\"\",\n" +
                "            \"kind\":\"sr\",\n" +
                "            \"orgId\":\"1\",\n" +
                "            \"parentSpanId\":\"0.1\",\n" +
                "            \"resultCode\":\"0\",\n" +
                "            \"rpcName\":\"com.aliyun.cbos.store.api.ProductRpcService@getItem\",\n" +
                "            \"rpcType\":\"8\",\n" +
                "            \"serverAppId\":\"1_1@811c06548ae3a13\",\n" +
                "            \"serverIp\":\"11.239.127.91\",\n" +
                "            \"spanId\":\"0.1.1\",\n" +
                "            \"timestamp\":1657511940232,\n" +
                "            \"topic\":\"span-arms-v3\",\n" +
                "            \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "}";

        JSONObject jsonObject1 = JSONObject.parseObject(temp);

        String temp1= "{\n" +
                "    \"empty\":false,\n" +
                "    \"labels\":[\n" +
                "        \"68e51083e737a3f7d9c9e7ab34b1f6a7\"\n" +
                "    ],\n" +
                "    \"spanIndex\":{\n" +
                "        \"0.1\":{\n" +
                "            \"appId\":\"1_1@10b8d040e1d0f78\",\n" +
                "            \"clientAppId\":\"1_1@a48b12f3ecc3d60\",\n" +
                "            \"clientIp\":\"11.239.127.90\",\n" +
                "            \"elapsed\":14,\n" +
                "            \"httpStatusCode\":\"200\",\n" +
                "            \"kind\":\"sr\",\n" +
                "            \"orgId\":\"1\",\n" +
                "            \"parentSpanId\":\"0\",\n" +
                "            \"resultCode\":\"0\",\n" +
                "            \"rpcName\":\"/api/CartRpcService/listItems\",\n" +
                "            \"rpcType\":\"0\",\n" +
                "            \"serverAppId\":\"1_1@10b8d040e1d0f78\",\n" +
                "            \"serverIp\":\"11.239.127.90\",\n" +
                "            \"spanId\":\"0.1\",\n" +
                "            \"timestamp\":1657511940232,\n" +
                "            \"topic\":\"span-arms-v3\",\n" +
                "            \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "}";

        JSONObject jsonObject2 = JSONObject.parseObject(temp1);

        String temp3 = "{\n" +
                "    \"empty\":false,\n" +
                "    \"labels\":[\n" +
                "        \"e118372a87c3d7af811e08c6eea829c6\"\n" +
                "    ],\n" +
                "    \"spanIndex\":{\n" +
                "        \"0\":{\n" +
                "            \"appId\":\"1_1@a48b12f3ecc3d60\",\n" +
                "            \"clientAppId\":\"\",\n" +
                "            \"clientIp\":\"\",\n" +
                "            \"elapsed\":20,\n" +
                "            \"httpStatusCode\":\"200\",\n" +
                "            \"kind\":\"sr\",\n" +
                "            \"orgId\":\"1\",\n" +
                "            \"parentSpanId\":\"\",\n" +
                "            \"resultCode\":\"0\",\n" +
                "            \"rpcName\":\"/api/cart/items/list\",\n" +
                "            \"rpcType\":\"0\",\n" +
                "            \"serverAppId\":\"1_1@a48b12f3ecc3d60\",\n" +
                "            \"serverIp\":\"11.239.127.90\",\n" +
                "            \"spanId\":\"0\",\n" +
                "            \"timestamp\":1657511940228,\n" +
                "            \"topic\":\"span-arms-v3\",\n" +
                "            \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "}";

        JSONObject jsonObject3 = JSONObject.parseObject(temp3);

        String temp4 = "{\n" +
                "    \"empty\":false,\n" +
                "    \"labels\":[\n" +
                "        \"68e51083e737a3f7d9c9e7ab34b1f6a7\"\n" +
                "    ],\n" +
                "    \"spanIndex\":{\n" +
                "        \"0.1.2\":{\n" +
                "            \"appId\":\"1_1@811c06548ae3a13\",\n" +
                "            \"clientAppId\":\"1_1@10b8d040e1d0f78\",\n" +
                "            \"clientIp\":\"11.239.127.90\",\n" +
                "            \"elapsed\":5,\n" +
                "            \"httpStatusCode\":\"\",\n" +
                "            \"kind\":\"sr\",\n" +
                "            \"orgId\":\"1\",\n" +
                "            \"parentSpanId\":\"0.1\",\n" +
                "            \"resultCode\":\"0\",\n" +
                "            \"rpcName\":\"com.aliyun.cbos.store.api.ProductRpcService@getItem\",\n" +
                "            \"rpcType\":\"8\",\n" +
                "            \"serverAppId\":\"1_1@811c06548ae3a13\",\n" +
                "            \"serverIp\":\"11.239.127.91\",\n" +
                "            \"spanId\":\"0.1.2\",\n" +
                "            \"timestamp\":1657511940234,\n" +
                "            \"topic\":\"span-arms-v3\",\n" +
                "            \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "        }\n" +
                "    },\n" +
                "    \"traceId\":\"ea1bef7f5a16575119402287990d0018\"\n" +
                "}";
        JSONObject jsonObject4 = JSONObject.parseObject(temp4);



        DataStreamSource source = StreamBuilder.dataStream("namespace", "pipeline");

        source.fromCollection(jsonObject1, jsonObject2, jsonObject3, jsonObject4)
                .map((message -> {
                    JSONObject spanIndex =(JSONObject) ((JSONObject) message).get("spanIndex");
                    JSONObject span = (JSONObject)spanIndex.getInnerMap().values().stream().findFirst().get();

                    Object spanId = span.get("spanId");
                    Object parentSpanId = span.get("parentSpanId");

                    ((JSONObject) message).put("spanId", spanId);
                    ((JSONObject) message).put("parentSpanId", parentSpanId);

                    return message;
                }))
                .window(TumblingWindow.of(Time.seconds(3)))
                .groupBy("traceId")
                .addUDAF(new TestUDAF(),"traceList","spanIndex")
                .setLocalStorageOnly(true)
                .toDataStream()
                .toPrint(1)
                .with(WindowStrategy.highPerformance())
                .start();
    }
}
