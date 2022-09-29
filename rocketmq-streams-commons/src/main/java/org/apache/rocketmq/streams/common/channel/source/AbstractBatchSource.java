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
package org.apache.rocketmq.streams.common.channel.source;

import com.alibaba.fastjson.JSONObject;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.rocketmq.streams.common.context.AbstractContext;
import org.apache.rocketmq.streams.common.context.BatchMessageOffset;
import org.apache.rocketmq.streams.common.context.Message;
import org.apache.rocketmq.streams.common.topology.builder.PipelineBuilder;
import org.apache.rocketmq.streams.common.utils.RuntimeUtil;

/**
 * 用批处理实现数据流 比如通过sql，定时获取数据，这类非消息队列数据源，没有offset和queueId，系统会模拟实现 也会增加offset的存储，实现断点续传
 */
public abstract class AbstractBatchSource extends AbstractSource {


    protected transient ScheduledExecutorService scheduled;


    protected transient AtomicLong offsetGenerator;


    protected transient long lastCommitTime;

    private transient BatchMessageOffset progress;//如果需要保存offset，通过这个对象保存

    public AbstractBatchSource() {
        setBatchMessage(true);
    }

    @Override
    protected boolean initConfigurable() {
        scheduled = new ScheduledThreadPoolExecutor(2);
        offsetGenerator = new AtomicLong(System.currentTimeMillis());
        long lastCommitTime = System.currentTimeMillis();
        return super.initConfigurable();
    }

    @Override
    protected boolean startSource() {
        String queueId = getQueueId();
        scheduled.scheduleWithFixedDelay(new Runnable() {
            @Override
            public void run() {
                if (System.currentTimeMillis() - lastCommitTime > getCheckpointTime()) {
                    lastCommitTime = System.currentTimeMillis();
                    sendCheckpoint(queueId);
                }

            }
        }, 0, getCheckpointTime(), TimeUnit.SECONDS);
        return true;
    }

    public AbstractContext doReceiveMessage(JSONObject message) {
        return doReceiveMessage(message, false);
    }

    @Override
    public JSONObject createJson(Object message) {
        if (isJsonData && message instanceof JSONObject) {
            return (JSONObject) message;
        }
        return super.createJson(message);

    }

    public AbstractContext doReceiveMessage(String message, boolean needFlush) {
        String queueId = getQueueId();
        String offset = this.offsetGenerator.incrementAndGet() + "";
        return doReceiveMessage(message, needFlush, queueId, offset);
    }

    public AbstractContext doReceiveMessage(JSONObject message, boolean needFlush) {
        String queueId = getQueueId();
        String offset = this.offsetGenerator.incrementAndGet() + "";
        return doReceiveMessage(message, needFlush, queueId, offset);
    }

    @Override
    public boolean supportNewSplitFind() {
        return false;
    }

    /**
     * 设置初始化参数和进度
     *
     * @param msg
     */
    public void setProgress(JSONObject msg) {
        BatchMessageOffset offset = new BatchMessageOffset();
        if (msg != null) {
            offset.setCurrentMessage(msg.toJSONString());
        }
        offset.setOwnerType(this.getType());
        this.progress = offset;
    }

    @Override
    public void addConfigurables(PipelineBuilder pipelineBuilder) {
        super.addConfigurables(pipelineBuilder);
        if (progress != null) {
            pipelineBuilder.addConfigurables(progress);
        }
    }

    /**
     * 提供单条消息的处理逻辑，默认不会加入checkpoint
     *
     * @param message
     * @return
     */
    @Override
    public AbstractContext doReceiveMessage(JSONObject message, boolean needSetCheckPoint, String queueId,
        String offset) {
        Message msg = createMessage(message, queueId, offset, needSetCheckPoint);
        return executeMessage(msg);
    }

    @Override
    protected boolean isNotDataSplit(String queueId) {
        return false;
    }

    /**
     * 对于批量接入的消息，可以在消息中加入checkpoint，在这批消息执行完成后，flush所有的输出节点，确保消息至少被消费一次
     *
     * @param messages          这批消息会作为一个批次
     * @param needSetCheckPoint 是否在最后一条消息加入checkpoint标志
     * @return
     */
    public AbstractContext doReceiveMessage(List<JSONObject> messages, boolean needSetCheckPoint) {
        if (messages == null || messages.size() == 0) {
            return null;
        }

        AbstractContext context = null;
        int i = 0;
        for (JSONObject jsonObject : messages) {

            if (i == messages.size() - 1) {
                doReceiveMessage(jsonObject, needSetCheckPoint);
            } else {
                doReceiveMessage(jsonObject, false);
            }
            i++;
        }
        return context;
    }

    @Override
    public boolean supportRemoveSplitFind() {
        return false;
    }

    @Override
    public boolean supportOffsetRest() {
        return false;
    }

    @Override
    public boolean isBatchMessage() {
        return true;
    }

    public String getQueueId() {
        return RuntimeUtil.getDipperInstanceId();
    }

    public Long createOffset() {
        return offsetGenerator.incrementAndGet();
    }
}
