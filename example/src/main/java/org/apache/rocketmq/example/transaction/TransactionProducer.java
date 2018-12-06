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
package org.apache.rocketmq.example.transaction;

import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.client.producer.SendResult;
import org.apache.rocketmq.client.producer.TransactionListener;
import org.apache.rocketmq.client.producer.TransactionMQProducer;
import org.apache.rocketmq.common.message.Message;
import org.apache.rocketmq.remoting.common.RemotingHelper;

import java.io.UnsupportedEncodingException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * RocketMQ的事务消息发送使用二阶段提交思路，首先，在消息发送时，先发送消息类型为Prepread类型的消息，
 * 然后在将该消息成功存入到消息服务器后，会回调   TransactionListener#executeLocalTransaction，
 * 执行本地事务状态回调函数，然后根据该方法的返回值，结束事务：
 *    1、COMMIT_MESSAGE ：提交事务。
 *    2、ROLLBACK_MESSAGE：回滚事务。
 *    3、UNKNOW：未知事务状态，此时消息服务器(Broker)收到EndTransaction命令时，将不对这种消息做处理，
 * 消息还处于Prepared类型，存储在主题为：RMQ_SYS_TRANS_HALF_TOPIC的队列中，然后消息发送流程将结束，
 * 那这些消息如何提交或回滚呢？为了实现避免客户端需要再次发送提交、回滚命令，RocketMQ会采取定时任务将
 * RMQ_SYS_TRANS_HALF_TOPIC中的消息取出，然后回到客户端，判断该消息是否需要提交或回滚，来完成事务消息的声明周期
 *
 *
 * 调用链路: 事物消息入口 TransactionMQProducer#sendMessageInTransaction
 *   -> DefaultMQProducerImpl#sendKernelImpl
 *    -> SendMessageProcessor#sendMessage
 *     -> TransactionalMessageServiceImpl#prepareMessage -> parseHalfMessageInner -> store.putMessage
 *    -> DefaultMQProducerImpl#sendMessageInTransaction
 *
 *  消息回查: TransactionalMessageCheckService#onWaitEnd
 *           ->
 */
public class TransactionProducer {
    /**
     *
     *  send 10条消息, 实际只消费3条消息,另外7条消息回滚
     *
     */
    public static void main(String[] args) throws MQClientException, InterruptedException {
        TransactionListener transactionListener = new TransactionListenerImpl();
        TransactionMQProducer producer = new TransactionMQProducer("please_rename_unique_group_name");
        ExecutorService executorService = new ThreadPoolExecutor(2, 5, 100, TimeUnit.SECONDS, new ArrayBlockingQueue<Runnable>(2000), new ThreadFactory() {
            @Override
            public Thread newThread(Runnable r) {
                Thread thread = new Thread(r);
                thread.setName("client-transaction-msg-check-thread");
                return thread;
            }
        });

        /**
         * 第一次提交到消息服务器，消息的主题被替换为RMQ_SYS_TRANS_HALF_TOPIC，当执行本地事务，
         * 如果返回本地事务状态为UN_KNOW时，第二次提交到服务器时将不会做任何操作，也就是消息还存在与RMQ_SYS_TRANS_HALF_TOPIC主题中，
         * 并不能被消息消费者消费，那这些消息最终如何被提交或回滚呢？
         *
         * RocketMQ使用TransactionalMessageCheckService线程定时去检测RMQ_SYS_TRANS_HALF_TOPIC主题中的消息，
         * 回查消息的事务状态。TransactionalMessageCheckService的检测频率默认1分钟，
         * 可通过在broker.conf文件中设置transactionCheckInterval的值来改变默认值，单位为毫秒。
         *
         */
        producer.setExecutorService(executorService);
        producer.setTransactionListener(transactionListener);
        producer.start();

        String[] tags = new String[] {"TagA", "TagB", "TagC", "TagD", "TagE"};
        for (int i = 0; i < 10; i++) {
            try {
                Message msg =
                    new Message("TopicTest1234", tags[i % tags.length], "KEY" + i,
                        ("Hello RocketMQ " + i).getBytes(RemotingHelper.DEFAULT_CHARSET));

                SendResult sendResult = producer.sendMessageInTransaction(msg, null);
                System.out.printf("%s%n", sendResult);

                Thread.sleep(10);
            } catch (MQClientException | UnsupportedEncodingException e) {
                e.printStackTrace();
            }
        }

        for (int i = 0; i < 100000; i++) {
            Thread.sleep(1000);
        }
        producer.shutdown();
    }
}
