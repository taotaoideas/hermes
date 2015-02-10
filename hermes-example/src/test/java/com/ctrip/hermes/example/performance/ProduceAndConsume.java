package com.ctrip.hermes.example.performance;

import java.io.IOException;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.engine.ConsumerManager;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;
import com.ctrip.hermes.remoting.netty.NettyServer;

public class ProduceAndConsume extends ComponentTestCase {

    static AtomicInteger count = new AtomicInteger(0);

    private void startServer() {
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
        }, 1000, 1000);
    }

    private void printAndClean() {
        System.out.println(String.format("Throughput: %d", count));
    }

    @Test
    public void myTest() throws IOException {
        startServer();
        startCountTimer();
        startProduceThread();
        startConsumeThread();

    }

    private void startConsumeThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                Producer p = lookup(Producer.class);

                for(int i = 1; i < 10; i++) {
                    p.message("order.new", "some data...").send();
                    count.addAndGet(1);
                }
            }
        }).start();
    }

    private void startProduceThread() {
        new Thread(new Runnable() {
            @Override
            public void run() {
                ConsumerManager m = lookup(ConsumerManager.class);

                CountDownLatch latch = new CountDownLatch(1);
                Subscriber s = new Subscriber("order.new", "groupId", new TestConsumer(latch));
                m.startConsumer(s);

                try {
                    latch.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }).start();
    }

    public static class TestConsumer implements Consumer<Object> {

        private CountDownLatch m_latch;
        public TestConsumer(CountDownLatch latch) {
            m_latch = latch;
        }

        @Override
        public void consume(List<Object> msgs) {
            System.out.println("Receive message: " + msgs);
            m_latch.countDown();
        }
    }
}
