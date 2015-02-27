package com.ctrip.hermes.example.performance;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.remoting.netty.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;

public class ProduceAndConsume extends ComponentTestCase {

    static AtomicInteger sendCount = new AtomicInteger(0);
    static AtomicInteger receiveCount = new AtomicInteger(0);

    final static long timeInterval = 3000;


    private void printAndClean() {
        int secondInTimeInterval = (int) timeInterval / 1000;
        System.out.println(String.format("Throughput:Send:%8d items, Receive: %8d items in %d second",
                sendCount.get(), receiveCount.get(), secondInTimeInterval));
        sendCount.set(0);
        receiveCount.set(0);
    }

    @Test
    public void myTest() throws IOException {
        startBroker();
        startCountTimer();
        startProduceThread();
        startConsumeThread();

        System.in.read();
    }

    private void startBroker() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                lookup(NettyServer.class).start();
            }
        }).start();
    }

    private void startCountTimer() {
        new Timer().schedule(new TimerTask() {
            @Override
            public void run() {
                printAndClean();
            }
        }, 1000, timeInterval);
    }

    private void startProduceThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Producer p = lookup(Producer.class);

                for (; ; ) {
                    p.message("order.new", sendCount.get()).send();
                    sendCount.addAndGet(1);
                    try {
                        Thread.sleep(1);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
            }
        }).start();
    }

    private void startConsumeThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                String topic = "order.new";
                ConsumerBootstrap b = lookup(ConsumerBootstrap.class);

                Subscriber s = new Subscriber(topic, "group1", new Consumer<String>() {
                    @Override
                    public void consume(List<Message<String>> msgs){
                        receiveCount.addAndGet(1);
                    }
                }, String.class);

                b.startConsumer(s);
            }
        }).start();
    }
}
