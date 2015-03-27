package com.ctrip.hermes.broker.channel;

// TODO copy cat related logic to other place
public class BrokerMessageChannelManager {

	// public static final String ID = "broker";
	//
	// @Inject
	// private MessageQueueManager m_queueManager;
	//
	// @Inject
	// private DeliverPipeline m_deliverPipeline;
	//
	// @Inject
	// private ReceiverPipeline m_receiverPipeline;
	//
	// private Map<Triple<String, String, String>, List<ConsumerChannelHandler>> m_handlers = new HashMap<>();
	//
	// private Logger m_logger;
	//
	// public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId) {
	// return newConsumerChannel(topic, groupId, "invalid");
	// }
	//
	// public synchronized ConsumerChannel newConsumerChannel(final String topic, final String groupId,
	// final String partition) {
	// return new ConsumerChannel() {
	//
	// private MessageQueue m_q = m_queueManager.findQueue(topic, groupId, partition);
	//
	// @Override
	// public void close() {
	// // TODO remove handler
	// }
	//
	// @Override
	// public void start(ConsumerChannelHandler handler) {
	// synchronized (BrokerMessageChannelManager.this) {
	// Triple<String, String, String> triple = new Triple<>(topic, groupId, partition);
	// List<ConsumerChannelHandler> curHandlers = m_handlers.get(triple);
	//
	// if (curHandlers == null) {
	// curHandlers = startQueuePuller(m_q, topic);
	// m_handlers.put(triple, curHandlers);
	// }
	// curHandlers.add(handler);
	// }
	// }
	//
	// @Override
	// public void ack(List<OffsetRecord> recs) {
	// m_q.ack(recs);
	// }
	// };
	//
	// }
	//
	// private List<ConsumerChannelHandler> startQueuePuller(final MessageQueue q, final String topic) {
	// // TODO
	// final List<ConsumerChannelHandler> handlers = Collections
	// .synchronizedList(new ArrayList<ConsumerChannelHandler>());
	//
	// new Thread() {
	//
	// private int m_idx = 0;
	//
	// @Override
	// public void run() {
	// while (true) {
	// List<Record> msgs = q.read(100);
	//
	// if (CollectionUtil.notEmpty(msgs)) {
	// dispatchMessage(msgs);
	// } else {
	// try {
	// Thread.sleep(100);
	// } catch (InterruptedException e) {
	// }
	// }
	// }
	// }
	//
	// @SuppressWarnings({ "rawtypes", "unchecked" })
	// private void dispatchMessage(List<Record> records) {
	// String ip = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
	// while (true) {
	// if (handlers.isEmpty()) {
	// // TODO shutdown and cleanup
	// try {
	// Thread.sleep(1000);
	// } catch (InterruptedException e) {
	// }
	// } else {
	// int curIdx = m_idx++;
	// final ConsumerChannelHandler handler = handlers.get(curIdx % handlers.size());
	// if (handler.isOpen()) {
	//
	// try {
	// List<StoredMessage<byte[]>> msgs = new ArrayList<>(records.size());
	// for (Record r : records) {
	// Transaction t = Cat.newTransaction("Message.Delivered", topic);
	// MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
	// String childMsgId = Cat.createMessageId();
	//
	// String rootMsgId = r.getProperty(CatConstants.ROOT_MESSAGE_ID);
	// String parentMsgId = r.getProperty(CatConstants.CURRENT_MESSAGE_ID);
	// String msgId = r.getProperty(CatConstants.SERVER_MESSAGE_ID);
	//
	// tree.setMessageId(msgId);
	// tree.setParentMessageId(parentMsgId);
	// tree.setRootMessageId(rootMsgId);
	//
	// Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childMsgId);
	// Cat.logEvent("Message:" + topic, "BrokerDelivered:" + ip, Event.SUCCESS, "key=" + r.getKey());
	//
	// r.setProperty(CatConstants.SERVER_MESSAGE_ID, childMsgId);
	// r.setProperty(CatConstants.CURRENT_MESSAGE_ID, msgId);
	// r.setProperty(CatConstants.ROOT_MESSAGE_ID, rootMsgId);
	//
	// msgs.add(new StoredMessage(r, topic));
	// t.setStatus(Transaction.SUCCESS);
	// t.complete();
	// }
	//
	// m_deliverPipeline.put(new Pair<>(msgs, new PipelineSink<Void>() {
	//
	// @Override
	// public Void handle(PipelineContext ctx, Object payload) {
	// List<StoredMessage<byte[]>> sinkMsgs = (List<StoredMessage<byte[]>>) payload;
	//
	// handler.handle(sinkMsgs);
	//
	// return null;
	// }
	// }));
	//
	// } catch (Exception e) {
	// // TODO handle exception
	// m_logger.error("", e);
	// } finally {
	// }
	// break;
	// } else {
	// m_logger.info(String.format("Remove closed consumer handler %s", handler));
	// handlers.remove(handler);
	// continue;
	// }
	// }
	// }
	// }
	//
	// }.start();
	//
	// return handlers;
	// }
	//
	// public void getProducerChannel(final String topic) {
	// final MessageQueue q = m_queueManager.findQueue(topic);
	//
	// // TODO
	// new Object() {
	//
	// public List<SendResult> send(final List<ProducerMessage<byte[]>> msgs) {
	// List<SendResult> result = new ArrayList<SendResult>(msgs.size());
	//
	// try {
	// m_receiverPipeline.put(new Pair<>(msgs, new PipelineSink<Void>() {
	//
	// @SuppressWarnings("unchecked")
	// @Override
	// public Void handle(PipelineContext<Void> ctx, Object payload) {
	// List<ProducerMessage<byte[]>> sinkMsgs = (List<ProducerMessage<byte[]>>) payload;
	//
	// final List<Record> records = new ArrayList<Record>();
	// String ip = NetworkInterfaceManager.INSTANCE.getLocalHostAddress();
	// for (ProducerMessage<byte[]> msg : sinkMsgs) {
	// Transaction t = Cat.newTransaction("Message.Received", topic);
	// MessageTree tree = Cat.getManager().getThreadLocalMessageTree();
	//
	// String childMsgId = Cat.createMessageId();
	// String msgId = msg.getProperty(CatConstants.SERVER_MESSAGE_ID);
	// String parentMsgId = msg.getProperty(CatConstants.CURRENT_MESSAGE_ID);
	// String rootMsgId = msg.getProperty(CatConstants.ROOT_MESSAGE_ID);
	//
	// tree.setMessageId(msgId);
	// tree.setParentMessageId(parentMsgId);
	// tree.setRootMessageId(rootMsgId);
	//
	// Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, "", Event.SUCCESS, childMsgId);
	// Cat.logEvent("Message:" + topic, "BrokerReceived:" + ip, Event.SUCCESS, "key=" + msg.getKey());
	//
	// msg.addProperty(CatConstants.CURRENT_MESSAGE_ID, msgId);
	// msg.addProperty(CatConstants.SERVER_MESSAGE_ID, childMsgId);
	//
	// records.add(new Record(msg));
	//
	// t.setStatus(Transaction.SUCCESS);
	// t.complete();
	// }
	// q.write(records);
	// return null;
	// }
	// }));
	//
	// } catch (Throwable e) {
	// m_logger.error("", e);
	// } finally {
	// for (ProducerMessage<byte[]> msg : msgs) {
	// // SendResult r = new SendResult();
	// // String msgId = (String) msg.getProperty(CatConstants.CURRENT_MESSAGE_ID);
	// // r.setCatMessageId(msgId);
	// // Cat.logEvent(CatConstants.TYPE_REMOTE_CALL, msgId);
	// // result.add(r);
	// }
	// }
	//
	// return result;
	// }
	//
	// };
	//
	// return null;
	// }

}
