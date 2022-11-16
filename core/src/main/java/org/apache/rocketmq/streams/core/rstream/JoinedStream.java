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
package org.apache.rocketmq.streams.core.rstream;

import org.apache.rocketmq.streams.core.function.KeySelectAction;
import org.apache.rocketmq.streams.core.function.ValueJoinAction;
import org.apache.rocketmq.streams.core.function.supplier.JoinWindowAggregateSupplier;
import org.apache.rocketmq.streams.core.running.Processor;
import org.apache.rocketmq.streams.core.runtime.operators.JoinType;
import org.apache.rocketmq.streams.core.runtime.operators.StreamType;
import org.apache.rocketmq.streams.core.runtime.operators.WindowInfo;
import org.apache.rocketmq.streams.core.topology.virtual.GraphNode;
import org.apache.rocketmq.streams.core.topology.virtual.ProcessorNode;
import org.apache.rocketmq.streams.core.util.OperatorNameMaker;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public class JoinedStream<V1, V2> {
    private RStream<V1> leftStream;
    private RStream<V2> rightStream;
    private JoinType joinType;

    public JoinedStream(RStream<V1> leftStream, RStream<V2> rightStream, JoinType joinType) {
        this.leftStream = leftStream;
        this.rightStream = rightStream;
        this.joinType = joinType;
    }

    public <K> Where<K> where(KeySelectAction<K, V1> rightKeySelectAction) {
        return new Where<>(rightKeySelectAction);
    }

    public class Where<K> {
        private KeySelectAction<K, V1> leftKeySelectAction;
        private KeySelectAction<K, V2> rightKeySelectAction;

        public Where(KeySelectAction<K, V1> leftKeySelectAction) {
            this.leftKeySelectAction = leftKeySelectAction;
        }


        public Where<K> equalTo(KeySelectAction<K, V2> rightKeySelectAction) {
            this.rightKeySelectAction = rightKeySelectAction;
            return this;
        }

        public JoinWindow<K> window(WindowInfo windowInfo) {
            return new JoinWindow<>(this.leftKeySelectAction, this.rightKeySelectAction, windowInfo);

        }
    }

    public class JoinWindow<K> {
        private KeySelectAction<K, V1> leftKeySelectAction;
        private KeySelectAction<K, V2> rightKeySelectAction;
        private WindowInfo windowInfo;

        public JoinWindow(KeySelectAction<K, V1> leftKeySelectAction, KeySelectAction<K, V2> rightKeySelectAction, WindowInfo windowInfo) {
            this.leftKeySelectAction = leftKeySelectAction;
            this.rightKeySelectAction = rightKeySelectAction;
            this.windowInfo = windowInfo;
        }

        public <OUT> RStream<OUT> apply(ValueJoinAction<V1, V2, OUT> joinAction) {
            List<String> temp = new ArrayList<>();
            WindowInfo.JoinStream joinStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, null);
            windowInfo.setJoinStream(joinStream);
            String name = OperatorNameMaker.makeName(OperatorNameMaker.JOIN_WINDOW_PREFIX);
            Supplier<Processor<? super OUT>> supplier = new JoinWindowAggregateSupplier<>(name, windowInfo, joinAction);
            ProcessorNode<OUT> commChild = new ProcessorNode(name, temp, supplier);

            Pipeline leftStreamPipeline = JoinedStream.this.leftStream.getPipeline();
            {
                GroupedStream<K, V1> leftGroupedStream = JoinedStream.this.leftStream.keyBy(leftKeySelectAction);

                WindowInfo leftWindowInfo = this.copy(windowInfo);

                WindowInfo.JoinStream leftStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, StreamType.LEFT_STREAM);
                leftWindowInfo.setJoinStream(leftStream);

                leftGroupedStream.window(leftWindowInfo);

                GraphNode lastNode = leftStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);
            }

            {

                GroupedStream<K, V2> rightGroupedStream = JoinedStream.this.rightStream.keyBy(rightKeySelectAction);

                WindowInfo rightWindowInfo = this.copy(windowInfo);

                WindowInfo.JoinStream leftStream = new WindowInfo.JoinStream(JoinedStream.this.joinType, StreamType.RIGHT_STREAM);
                rightWindowInfo.setJoinStream(leftStream);

                rightGroupedStream.window(rightWindowInfo);

                Pipeline rightStreamPipeline = JoinedStream.this.rightStream.getPipeline();

                GraphNode lastNode = rightStreamPipeline.getLastNode();
                temp.add(lastNode.getName());
                commChild.addParent(lastNode);

                lastNode.addChild(commChild);
            }

            return new RStreamImpl<>(leftStreamPipeline, commChild);
        }

        private WindowInfo copy(WindowInfo windowInfo) {
            WindowInfo result = new WindowInfo();

            WindowInfo.JoinStream joinStream = windowInfo.getJoinStream();

            if (joinStream != null) {
                WindowInfo.JoinStream stream = new WindowInfo.JoinStream(joinStream.getJoinType(), joinStream.getStreamType());
                result.setJoinStream(stream);
            }

            result.setSessionTimeout(windowInfo.getSessionTimeout());
            result.setWindowType(windowInfo.getWindowType());
            result.setWindowSize(windowInfo.getWindowSize());
            result.setWindowSlide(windowInfo.getWindowSlide());

            return result;
        }


    }
}