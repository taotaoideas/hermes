package com.ctrip.hermes.example.feature.producer;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.component.repository.exception.ComponentLookupException;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.producer.Producer;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class OneProducer extends ComponentTestCase {

    private final int msgBatchCount = 100;
    static final String TOPIC = "order.new";

    public void startBroker() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                lookup(NettyServer.class).start();
            }
        }).start();
    }

    @Test
    public void SendOneMsg() throws ComponentLookupException, InterruptedException {
        startBroker();

        final String msg = "Feature Test: SendOneMsg()";
        final CountDownLatch latch = new CountDownLatch(1);

        Producer p = lookup(Producer.class);
        p.message(TOPIC, msg).send();

        ConsumerBootstrap b = lookup(ConsumerBootstrap.class);
        Subscriber s = new Subscriber(TOPIC, "group1", new Consumer<String>() {
            @Override
            public void consume(List<StoredMessage<String>> msgs) {
                assertEquals(msgs.size(), 1);
                assertEquals(msgs.get(0).getBody(), msg);
                latch.countDown();
            }
        }, String.class);

        b.startConsumer(s);

        assertTrue(latch.await(1, TimeUnit.SECONDS));
    }


    @Test
    public void SendManyMsg() throws ComponentLookupException, InterruptedException {
        startBroker();
        // todo
    }


    /**
     * 发不同优先级的msgs:
     * 1, 对于已经存在队列中的，高优先级先收到；
     * 2, 新来的高优先级，优先被发送。       //不易在client端实现，应在server端test.
     */
    @Test
    public void HighPriorityFirstReceived() throws ComponentLookupException, InterruptedException {
//        OldProducer oldProducer = buildSyncProducer(factory);
//
//
//        final List<Message> receivedMsgs = new ArrayList<>();
//        final CountDownLatch latch = new CountDownLatch(3 * msgBatchCount);
//        OldConsumer oldConsumer = buildConsumer(factory);
//        oldConsumer.setMessageListener(new MessageListener() {
//            @Override
//            public void onMessage(Message msg) {
//                System.out.println(msg.getPriority());
//                receivedMsgs.add(msg);
//                latch.countDown();
//            }
//        });
//        oldConsumer.start();
//        oldConsumer.stop();
//
//        batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.LOW);
//        batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.MIDDLE);
//        batchSendMsgs(oldProducer, msgBatchCount, MessagePriority.HIGH);
//
//        oldConsumer.start();
//        assertTrue(latch.await(5, TimeUnit.SECONDS));
//
//        for (int i = 0; i < msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.HIGH);
//        }
//        for (int i = msgBatchCount; i < 2 * msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.MIDDLE);
//        }
//        for (int i = 2 * msgBatchCount; i < 3 * msgBatchCount; i++) {
//            assertEquals(receivedMsgs.get(i).getPriority(), MessagePriority.LOW);
//        }
    }

    /**
     * 固定时间之后才能够收到的msg
     * TODO: to be implemented.
     */
    @Test
    public void produceDelayedMsgs() {
    }

}
