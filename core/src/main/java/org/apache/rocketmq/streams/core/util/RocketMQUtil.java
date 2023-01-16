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
package org.apache.rocketmq.streams.core.util;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.PosixParser;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.MixAll;
import org.apache.rocketmq.common.TopicConfig;
import org.apache.rocketmq.common.constant.PermName;
import org.apache.rocketmq.common.protocol.ResponseCode;
import org.apache.rocketmq.common.protocol.body.ClusterInfo;
import org.apache.rocketmq.remoting.exception.RemotingException;
import org.apache.rocketmq.srvutil.ServerUtil;
import org.apache.rocketmq.tools.admin.DefaultMQAdminExt;
import org.apache.rocketmq.tools.command.CommandUtil;
import org.apache.rocketmq.tools.command.topic.UpdateTopicSubCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

public class RocketMQUtil {
    private static final Logger logger = LoggerFactory.getLogger(RocketMQUtil.class.getName());

    private static final List<String> existTopic = new ArrayList<>();

    //neither static topic nor compact topic.
    public static void createNormalTopic(DefaultMQAdminExt mqAdmin, String topicName, int queueNum, Set<String> clusters) throws Exception {
        if (check(mqAdmin, topicName)) {
            logger.info("topic[{}] already exist.", topicName);
            return;
        }

        if (clusters == null || clusters.size() == 0) {
            clusters = getCluster(mqAdmin);
        }

        TopicConfig topicConfig = new TopicConfig(topicName, queueNum, queueNum, PermName.PERM_READ | PermName.PERM_WRITE);

        for (String cluster : clusters) {
            Set<String> masterSet = CommandUtil.fetchMasterAddrByClusterName(mqAdmin, cluster);

            for (String addr : masterSet) {
                mqAdmin.createAndUpdateTopicConfig(addr, topicConfig);
                logger.info("create topic to broker:{} cluster:{}, success.", addr, cluster);
            }
        }
    }

    public static void createNormalTopic(DefaultMQAdminExt mqAdmin, String topicName, int queueNum) throws Exception {
        Set<String> clusters = getCluster(mqAdmin);
        createNormalTopic(mqAdmin, topicName, queueNum, clusters);
    }


    private static void update2CompactTopicWithCommand(String topic, int queueNum, String cluster, String nameservers) throws Exception {
        UpdateTopicSubCommand command = new UpdateTopicSubCommand();
        Options options = ServerUtil.buildCommandlineOptions(new Options());
        String[] args = new String[]{
                "-c", cluster,
                "-t", topic,
                "-r", String.valueOf(queueNum),
                "-w", String.valueOf(queueNum),
                "-n", nameservers
//                todo 发布版本还不支持
//                , "-a", "+delete.policy=COMPACTION"
        };

        final CommandLine commandLine = ServerUtil.parseCmdLine("mqadmin " + command.commandName(), args, command.buildCommandlineOptions(options), new PosixParser());
        String namesrvAddr = commandLine.getOptionValue('n');
        System.setProperty(MixAll.NAMESRV_ADDR_PROPERTY, namesrvAddr);

        command.execute(commandLine, options, null);
    }


    private static Set<String> getCluster(DefaultMQAdminExt mqAdmin) throws Exception {
        ClusterInfo clusterInfo = mqAdmin.examineBrokerClusterInfo();
        return clusterInfo.getClusterAddrTable().keySet();
    }

    private static boolean check(DefaultMQAdminExt mqAdmin, String topicName) {
        if (existTopic.contains(topicName)) {
            return true;
        }

        try {
            mqAdmin.examineTopicRouteInfo(topicName);
            existTopic.add(topicName);
            return true;
        } catch (RemotingException | InterruptedException e) {
            logger.error("examine topic route info error.", e);
            throw new RuntimeException("examine topic route info error.", e);
        } catch (MQClientException exception) {
            if (exception.getResponseCode() == ResponseCode.TOPIC_NOT_EXIST) {
                logger.info("topic[{}] does not exist, create it.", topicName);
            } else {
                throw new RuntimeException(exception);
            }
        }
        return false;
    }

    public static boolean checkWhetherExist(String topic) {
        return existTopic.contains(topic);
    }
}
