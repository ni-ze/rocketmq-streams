package org.apache.rocketmq.streams.running;
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

import org.apache.rocketmq.client.consumer.DefaultLitePullConsumer;
import org.apache.rocketmq.client.consumer.MessageQueueListener;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.message.MessageQueue;
import org.apache.rocketmq.streams.common.Constant;
import org.apache.rocketmq.streams.serialization.Serde;
import org.apache.rocketmq.streams.topology.TopologyBuilder;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;

import java.util.HashSet;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.Function;

import static org.apache.rocketmq.common.message.MessageConst.PROPERTY_KEYS;
import static org.apache.rocketmq.streams.metadata.StreamConfig.ROCKETMQ_STREAMS_CONSUMER_GROUP;

public class WorkerThread extends Thread {
    private final TopologyBuilder topologyBuilder;
    private final Engine<?> engine;
    private final Properties properties;

    public WorkerThread(TopologyBuilder topologyBuilder, Properties properties) throws MQClientException {
        this.topologyBuilder = topologyBuilder;
        this.properties = properties;

        RocketMQClient rocketMQClient = new RocketMQClient(properties.getProperty(MixAll.NAMESRV_ADDR_PROPERTY));

        Set<String> sourceTopic = topologyBuilder.getSourceTopic();
        DefaultLitePullConsumer unionConsumer = rocketMQClient.pullConsumer(ROCKETMQ_STREAMS_CONSUMER_GROUP, sourceTopic);

        MessageQueueListener originListener = unionConsumer.getMessageQueueListener();
        MessageQueueListenerWrapper wrapper = new MessageQueueListenerWrapper(originListener, topologyBuilder);
        unionConsumer.setMessageQueueListener(wrapper);

        DefaultMQProducer producer = rocketMQClient.producer(ROCKETMQ_STREAMS_CONSUMER_GROUP);
        DefaultMQAdminExt mqAdmin = rocketMQClient.getMQAdmin();


        this.engine = new Engine<>(unionConsumer, producer, mqAdmin, wrapper::selectProcessor, wrapper::buildKey);
    }

    @Override
    public void run() {
        try {
            this.engine.start();

            this.engine.runInLoop();
        } catch (Throwable e) {
            e.printStackTrace();
        } finally {
            this.engine.stop();
        }
    }


    @SuppressWarnings("unchecked")
    class Engine<T> {
        private final DefaultLitePullConsumer unionConsumer;
        private final DefaultMQProducer producer;
        private final DefaultMQAdminExt mqAdmin;
        private final Function<String, Processor<T>> processorSelect;
        private final BiFunction<String, Integer, String> keyBuilder;
        private volatile boolean running = false;

        public Engine(DefaultLitePullConsumer unionConsumer, DefaultMQProducer producer,
                      DefaultMQAdminExt mqAdmin,
                      Function<String, Processor<T>> processorSelect, BiFunction<String, Integer, String> keyBuilder) {
            this.unionConsumer = unionConsumer;
            this.producer = producer;
            this.mqAdmin = mqAdmin;
            this.processorSelect = processorSelect;
            this.keyBuilder = keyBuilder;
        }

        //todo 恢复状态


        //处理
        public void start() {
            try {
                this.unionConsumer.start();
                this.producer.start();
                this.mqAdmin.start();


            } catch (MQClientException e) {
                //todo
                e.printStackTrace();
            }
        }

        public void runInLoop() throws Throwable {
            if (running) {
                return;
            }

            running = true;
            //todo 1、阻塞等待分配了哪些MQ
            //todo 2、然后加载状态

            while (running) {
                List<MessageExt> list = this.unionConsumer.poll(0);
                if (list.size() == 0) {
                    Thread.sleep(100);
                    continue;
                }

                HashSet<MessageQueue> set = new HashSet<>();
                for (MessageExt messageExt : list) {
                    byte[] body = messageExt.getBody();
                    if (body == null || body.length == 0) {
                        continue;
                    }

                    Serde<T> serde = getSerde();
                    T value = makeValue(serde, messageExt);

                    String topic = messageExt.getTopic();
                    int queueId = messageExt.getQueueId();
                    String brokerName = messageExt.getBrokerName();
                    MessageQueue queue = new MessageQueue(topic, brokerName, queueId);
                    set.add(queue);


                    String key = keyBuilder.apply(topic, queueId);
                    Processor<T> processor = this.processorSelect.apply(key);

                    StreamContextImpl<T> context = new StreamContextImpl<>(serde, producer, mqAdmin, messageExt);
                    context.setKey(messageExt.getProperty(PROPERTY_KEYS));

                    processor.preProcess(context);
                    processor.process(value);
                }

                //todo 每次都提交位点消耗太大，后面改成拉取消息放入buffer的形式。
                this.unionConsumer.commit(set, true);
            }
        }

        private Serde<T> getSerde() throws Throwable {
            String serDe = WorkerThread.this.properties.getProperty(Constant.DEFAULT_SERDE_CLASS_CONFIG);
            Class<?> serDeClazz = Class.forName(serDe);
            if (!Serde.class.isAssignableFrom(serDeClazz)) {
                throw new IllegalArgumentException("serde class must implements interface org.apache.rocketmq.streams.serialization.Serde");
            }

            return (Serde<T>) serDeClazz.getDeclaredConstructor().newInstance();
        }


        private T makeValue(Serde<T> serde, MessageExt messageExt) throws Throwable {
            byte[] body = messageExt.getBody();
            return serde.deserializer().deserialize(body);
        }

        public void stop() {
            this.running = false;

            try {
                this.unionConsumer.shutdown();
                this.producer.shutdown();
                this.mqAdmin.shutdown();
            } catch (Throwable e) {
                //todo
                e.printStackTrace();
            }
        }
    }


}
