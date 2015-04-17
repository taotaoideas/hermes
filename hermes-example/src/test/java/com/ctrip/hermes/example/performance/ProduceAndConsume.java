package com.ctrip.hermes.example.performance;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.transport.NettyServer;
import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.engine.Engine;
import com.ctrip.hermes.consumer.engine.Subscriber;
import com.ctrip.hermes.core.message.ConsumerMessage;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.producer.api.Producer;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.SettableFuture;

public class ProduceAndConsume extends ComponentTestCase {

	static AtomicInteger sendCount = new AtomicInteger(0);

	static AtomicInteger receiveCount = new AtomicInteger(0);

	static AtomicInteger totalSend = new AtomicInteger(0);

	static AtomicInteger totalReceive = new AtomicInteger(0);

	final static long timeInterval = 3000;

	final static String TOPIC = "order_new";

	private void printAndClean() {
		int secondInTimeInterval = (int) timeInterval / 1000;

		totalSend.addAndGet(sendCount.get());
		totalReceive.addAndGet(receiveCount.get());
		System.out.println(String.format(
		      "Throughput:Send:%8d items (QPS: %.2f msg/s), Receive: %8d items (QPS: %.2f msg/s) " + "in %d "
		            + "second. " + "Total Send: %8d, Total Receive: %8d, Delta: %8d.", sendCount.get(), sendCount.get()
		            / (float) secondInTimeInterval, receiveCount.get(),
		      receiveCount.get() / (float) secondInTimeInterval, secondInTimeInterval, totalSend.get(),
		      totalReceive.get(), Math.abs(totalSend.get() - totalReceive.get())));

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
		try {
	      TimeUnit.SECONDS.sleep(2);
      } catch (InterruptedException e) {
	      // TODO Auto-generated catch block
	      e.printStackTrace();
      }
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
		runProducer();
	}

	private void runProducer() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				Producer p = lookup(Producer.class);

				for (;;) {
					SettableFuture<SendResult> future = (SettableFuture<SendResult>) p.message(TOPIC, sendCount.get())
					      .send();
					Futures.addCallback(future, new FutureCallback<SendResult>() {

						@Override
                  public void onSuccess(SendResult result) {
							sendCount.addAndGet(1);
                  }

						@Override
                  public void onFailure(Throwable t) {
	                  
                  }
						
					});
				}
			}
		}).start();
	}

	private void startConsumeThread() {
		new Thread(new Runnable() {
			@Override
			public void run() {
				String topic = TOPIC;
				Engine engine = lookup(Engine.class);

				Subscriber s = new Subscriber(topic, "group1", new Consumer<String>() {
					@Override
					public void consume(List<ConsumerMessage<String>> msgs) {
						receiveCount.addAndGet(msgs.size());
						for (ConsumerMessage<?> msg : msgs) {
							// TODO
							msg.ack();
						}
					}
				});

				engine.start(Arrays.asList(s));
			}
		}).start();
	}
}
