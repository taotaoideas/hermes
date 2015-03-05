package com.ctrip.hermes.local;

import org.mortbay.jetty.Server;
import org.mortbay.jetty.webapp.WebAppContext;

public class LocalDevServer {

	private static LocalDevServer instance = new LocalDevServer();

	public static LocalDevServer getInstance() {
		return instance;
	}

	private LocalDevServer() {

	}

	public void start() throws Exception {
		Server server = new Server(2765);
		WebAppContext ctx = new WebAppContext();

		ctx.setContextPath("/");
		ctx.setWar(this.getClass().getResource("/webapp").toString());
		server.setHandler(ctx);

		server.start();

	}

	public static void main(String[] args) throws Exception {
		LocalDevServer.getInstance().start();
	}

	public void startWebapp() throws Exception {
		// open the page in the default browser
		// display("/index.html");

		// startConsumerGroup("order.new", "Group No.1", "My Consumer Name 1");
		// startConsumerGroup("order.new", "Group No.1", "My Consumer Name 2");
		// startConsumerGroup("order.new", "Group No.2", "My Consumer Name 3");
		// startConsumerGroup("order.new", "Group No.2", "My Consumer Name 4");
		// startConsumerGroup("order.new", "Group No.2", "My Consumer Name 5");
		// startConsumerGroup("order.new", "Group No.3", "My Consumer Name 6");
		//
		// startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");
		// startConsumerGroup("local.order.new", "Group No.3", "My Consumer Name 6");
		//
		// startConsumerGroup("test.topic", "Group No.3", "My Consumer Name 6");
	}

	// private void startConsumerGroup(final String topic, final String group, String consumerName) {
	//
	// MockConsumers.getInstance().putOneConsumer(topic, group, consumerName);
	//
	// ConsumerChannel cc = m_channelManager.newConsumerChannel(topic, group);
	// cc.start(new ConsumerChannelHandler() {
	// @Override
	// public void handle(List<StoredMessage<byte[]>> msgs) {
	// for (StoredMessage<byte[]> msg : msgs) {
	// MockConsumers.getInstance().consumeOneMsg(topic, group, msg);
	// }
	// }
	//
	// @Override
	// public boolean isOpen() {
	// return true;
	// }
	// });
	// }
}