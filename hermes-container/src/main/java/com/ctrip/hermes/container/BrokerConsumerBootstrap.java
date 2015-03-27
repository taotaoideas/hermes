package com.ctrip.hermes.container;

import org.unidal.lookup.ContainerHolder;

public class BrokerConsumerBootstrap extends ContainerHolder /* implements LogEnabled, ConsumerBootstrap, Initializable */{

	// public static final String ID = "broker";
	//
	// @Inject
	// private ValveRegistry m_valveRegistry;
	//
	// @Inject
	// private Pipeline<Void> m_pipeline;
	//
	// @Inject
	// private ClientManager m_clientManager;
	//
	// private Logger m_logger;
	//
	// private Map<Long, SinkContext> m_consumerSinks = new ConcurrentHashMap<>();
	//
	// private Map<Long, AckContext> m_acks = new ConcurrentHashMap<>();
	//
	// @Override
	// public void startConsumer(Subscriber s) {
	// NettyClientHandler netty = m_clientManager.findConsumerClient(s.getTopicPattern(), s.getGroupId());
	//
	// Command cmd = new Command(CommandType.StartConsumerRequest) //
	// .addHeader("topic", s.getTopicPattern()) //
	// .addHeader("groupId", s.getGroupId());
	//
	// LinkedBlockingQueue<OffsetRecord> ackQueue = new LinkedBlockingQueue<>();
	// m_acks.put(cmd.getCorrelationId(), new AckContext(ackQueue, netty));
	// m_consumerSinks.put(cmd.getCorrelationId(), new SinkContext(s, newConsumerSink(s, ackQueue)));
	//
	// ChannelFuture future = netty.writeCommand(cmd);
	//
	// // TODO
	// future.awaitUninterruptibly();
	// }
	//
	// private PipelineSink<Void> newConsumerSink(final Subscriber s, final LinkedBlockingQueue<OffsetRecord> ackQueue) {
	// return new PipelineSink<Void>() {
	//
	// @SuppressWarnings({ "unchecked", "rawtypes" })
	// @Override
	// public Void handle(PipelineContext ctx, Object payload) {
	// List<StoredMessage> msgs = (List<StoredMessage>) payload;
	// // TODO
	// try {
	// s.getConsumer().consume(msgs);
	// } catch (Throwable e) {
	// // TODO add more message detail
	// m_logger.warn("Consumer throws exception when consuming messge", e);
	// } finally {
	// // TODO extract offset record from payload
	// for (StoredMessage msg : msgs) {
	// OffsetRecord offsetRecord = new OffsetRecord(msg.getOffset(), msg.getAckOffset());
	// Ack ack = msg.isSuccess() ? Ack.SUCCESS : Ack.FAIL;
	// offsetRecord.setAck(ack);
	// ackQueue.offer(offsetRecord);
	// }
	// }
	//
	// return null;
	// }
	// };
	// }
	//
	// @Override
	// public void deliverMessage(long correlationId, List<StoredMessage<byte[]>> msgs) {
	// // TODO make it async
	// SinkContext sinkCtx = m_consumerSinks.get(correlationId);
	// PipelineSink<Void> sink = sinkCtx.getSink();
	// Subscriber s = sinkCtx.getSubscriber();
	//
	// if (sink != null) {
	// MessageContext ctx = new MessageContext(s.getTopicPattern(), msgs, s.getMessageClass());
	// m_pipeline.put(new Pair<>(sink, ctx));
	// } else {
	// // TODO
	// m_logger.error(String.format("Correlationid %s not found", correlationId));
	// }
	// }
	//
	// @Override
	// public void enableLogging(Logger logger) {
	// m_logger = logger;
	// }
	//
	// @Override
	// public void initialize() throws InitializationException {
	// // TODO
	// new Thread() {
	//
	// @Override
	// public void run() {
	// while (true) {
	// // TODO
	// for (Map.Entry<Long, AckContext> entry : m_acks.entrySet()) {
	// entry.getValue().send(entry.getKey());
	// }
	// try {
	// Thread.sleep(100);
	// } catch (InterruptedException e) {
	// }
	// }
	// }
	//
	// }.start();
	//
	// }
	//
	// private static class SinkContext {
	//
	// private Subscriber m_subscriber;
	//
	// private PipelineSink<Void> m_sink;
	//
	// public SinkContext(Subscriber subscriber, PipelineSink<Void> sink) {
	// m_subscriber = subscriber;
	// m_sink = sink;
	// }
	//
	// public Subscriber getSubscriber() {
	// return m_subscriber;
	// }
	//
	// public PipelineSink<Void> getSink() {
	// return m_sink;
	// }
	//
	// }
	//
	// private static class AckContext {
	//
	// private LinkedBlockingQueue<OffsetRecord> m_ackQueue;
	//
	// private NettyClientHandler m_netty;
	//
	// public AckContext(LinkedBlockingQueue<OffsetRecord> ackQueue, NettyClientHandler netty) {
	// m_ackQueue = ackQueue;
	// m_netty = netty;
	// }
	//
	// public void send(long correlationId) {
	// OffsetRecord rec = null;
	// while ((rec = m_ackQueue.poll()) != null) {
	// // TODO
	// Command cmd = new Command(CommandType.AckRequest) //
	// .setCorrelationId(correlationId) //
	// .setBody(JSON.toJSONBytes(rec));
	//
	// m_netty.writeCommand(cmd);
	// }
	// }
	//
	// }

}
