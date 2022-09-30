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


    public AbstractContext doReceiveMessage(JSONObject message, boolean needFlush) {
        String queueId = getQueueId();
        String offset = this.offsetGenerator.incrementAndGet() + "";
        return doReceiveMessage(message, needFlush, queueId, offset);
    }

    @Override
    public boolean supportNewSplitFind() {
        return false;
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

 }
