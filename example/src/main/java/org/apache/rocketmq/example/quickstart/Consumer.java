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
package org.apache.rocketmq.example.quickstart;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyContext;
import org.apache.rocketmq.client.consumer.listener.ConsumeConcurrentlyStatus;
import org.apache.rocketmq.client.consumer.listener.MessageListenerConcurrently;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.consumer.ConsumeFromWhere;
import org.apache.rocketmq.common.message.MessageExt;

/**
 * This example shows how to subscribe and consume messages using providing {@link DefaultMQPushConsumer}.
 */
public class Consumer {

    private static Set<String> key = new HashSet<>();

    public static void main(String[] args) throws InterruptedException, MQClientException {
        newConsumer("sparrow-sender");
        //newConsumer("consumer-group-1");
        //newConsumer("consumer-group-2");
    }

    private static void newConsumer(String consumerName) throws MQClientException { /*
     * Instantiate with specified consumer group name.
     */
        final DefaultMQPushConsumer consumer = new DefaultMQPushConsumer(consumerName);

        /*
         * Specify name server addresses.
         * <p/>
         *
         * Alternatively, you may specify name server addresses via exporting environmental variable: NAMESRV_ADDR
         * <pre>
         * {@code
         * consumer.setNamesrvAddr("name-server1-ip:9876;name-server2-ip:9876");
         * }
         * </pre>
         */
        consumer.setNamesrvAddr("127.0.0.1:9876");
        /*
         * Specify where to start in case the specified consumer group is a brand new one.
         */
        consumer.setConsumeFromWhere(ConsumeFromWhere.CONSUME_FROM_FIRST_OFFSET);

        /*
         * Subscribe one more more topics to consume.
         */
        consumer.subscribe("sparrow-topic", "*");

        consumer.setConsumeThreadMin(2);
        consumer.setPullInterval(10000);
        consumer.setPullBatchSize(100);
        consumer.setConsumeMessageBatchMaxSize(3);
        consumer.setPullThresholdForQueue(2000);
        //pullThresholdForQueue

//        new Thread(new Runnable() {
//            @Override
//            public void run() {
//                boolean is10=true;
//                while (true) {
//                    try {
//                        Thread.sleep(10000L);
//                    } catch (InterruptedException e) {
//                        e.printStackTrace();
//                    }
//                    if(is10) {
//                        consumer.setPullInterval(0);
//                        is10=!is10;
//                    }else {
//                        consumer.setPullInterval(10000L);
//                        is10=!is10;
//                    }
//                }
//            }
//        }).start();


        /*
         *  Register callback to execute on arrival of messages fetched from brokers.
         */
        consumer.registerMessageListener(new MessageListenerConcurrently() {

            @Override
            public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs,
                                                            ConsumeConcurrentlyContext context) {
                for (MessageExt messageExt : msgs) {
                    System.out.println("consumer "+msgs);
                    if (key.contains(messageExt.getMsgId())) {
                        System.out.println("exist" + messageExt.getMsgId());
                    } else {
                        key.add(messageExt.getMsgId());
                    }
                }

//                if(msgs.size()<500){
//                    consumer.setPullInterval(10000);
//                }
//                else {
//                    consumer.setPullInterval(100);
//                }
                System.err.println(System.currentTimeMillis() / 1000 + Thread.currentThread().getName() + " message size :" + msgs.size());
                //System.out.printf("%s Receive New Messages: %s %n", Thread.currentThread().getName(), msgs);
                try {
                    Thread.sleep(500);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
                return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
            }
        });

        /*
         *  Launch the consumer instance.
         */
        consumer.start();

        System.out.printf("Consumer Started.%n");
    }
}
