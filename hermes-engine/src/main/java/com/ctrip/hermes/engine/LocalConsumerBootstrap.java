package com.ctrip.hermes.engine;

import java.util.Arrays;
import java.util.List;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.unidal.lookup.annotation.Inject;
import org.unidal.tuple.Pair;

import com.ctrip.hermes.channel.ConsumerChannel;
import com.ctrip.hermes.channel.ConsumerChannelHandler;
import com.ctrip.hermes.channel.MessageChannelManager;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.message.PipelineSink;
import com.ctrip.hermes.storage.message.Ack;
import com.ctrip.hermes.storage.range.OffsetRecord;

public class LocalConsumerBootstrap implements ConsumerBootstrap, LogEnabled {

	public static final String ID = "local";

	@Inject
	private MessageChannelManager m_channelManager;

	@Inject
	private ConsumerPipeline m_pipeline;

	private Logger m_logger;

	@Override
	public void startConsumer(final Subscriber s) {
		final String topic = s.getTopicPattern();
		final ConsumerChannel cc = m_channelManager.newConsumerChannel(topic, s.getGroupId());

		cc.start(new ConsumerChannelHandler() {

			@Override
			public boolean isOpen() {
				return true;
			}

			@Override
			public void handle(List<com.ctrip.hermes.storage.message.Message> smsgs) {
				Pair<PipelineSink, Object> pair = new Pair<>();
				pair.setKey(new PipelineSink() {

					@SuppressWarnings({ "rawtypes", "unchecked" })
					@Override
					public void handle(PipelineContext ctx, Object payload) {
						List<Message> msgs = (List<Message>) payload;
						// TODO
						try {
							s.getConsumer().consume(msgs);
						} catch (Throwable e) {
							// TODO add more message detail
							m_logger.warn("Consumer throws exception when consuming messge", e);
						} finally {
							// TODO extract offset record from payload
							for (Message msg : msgs) {
								OffsetRecord offsetRecord = new OffsetRecord(msg.getOffset(), msg.getAckOffset());
								Ack ack = msg.isSuccess() ? Ack.SUCCESS : Ack.FAIL;
								offsetRecord.setAck(ack);
								cc.ack(Arrays.asList(offsetRecord));
							}
						}
					}
				});
				pair.setValue(new MessageContext(topic, smsgs));

				m_pipeline.put(pair);
			}
		});
	}

	@Override
	public void deliverMessage(int correlationId, MessageContext ctx) {
		// won't get called
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}

}
