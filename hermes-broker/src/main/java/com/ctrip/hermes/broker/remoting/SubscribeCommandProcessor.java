package com.ctrip.hermes.broker.remoting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.List;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MTopicShardPriority;
import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.broker.queue.QueueReader;
import com.ctrip.hermes.core.message.ConsumerMessageBatch;
import com.ctrip.hermes.core.message.PartialDecodedMessage;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodecFactory;
import com.ctrip.hermes.core.transport.TransferCallback;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.ConsumeMessageCommand;
import com.ctrip.hermes.core.transport.command.SubscribeCommand;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.ctrip.hermes.storage.util.CollectionUtil;
import com.google.common.base.Charsets;

public class SubscribeCommandProcessor implements CommandProcessor {

	@Inject
	private MessageQueueManager m_queueManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.SUBSCRIBE);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		startPollingTask(ctx);
	}

	private void startPollingTask(final CommandProcessorContext ctx) {
		final SubscribeCommand req = (SubscribeCommand) ctx.getCommand();
		final QueueReader reader = m_queueManager.createReader(req.getTopic(), req.getPartition());
		new Thread() {
			public void run() {
				long startMsgId = 0;
				while (true) {
					final List<MTopicShardPriority> dataObjs = reader.read(startMsgId, 10);

					if (CollectionUtil.notEmpty(dataObjs)) {
						startMsgId = CollectionUtil.last(dataObjs).getId();

						final ConsumerMessageBatch batch = new ConsumerMessageBatch();
						batch.setTransferCallback(new TransferCallback() {

							@Override
							public void transfer(ByteBuf out) {
								for (MTopicShardPriority dataObj : dataObjs) {
									batch.addMsgSeq(dataObj.getId());

									MessageCodec codec = MessageCodecFactory.getCodec(req.getTopic());
									PartialDecodedMessage partialMsg = new PartialDecodedMessage();
									partialMsg.setAppProperties(stringToByteBuf(dataObj.getAttributes()));
									partialMsg.setBody(Unpooled.wrappedBuffer(dataObj.getPayload()));
									partialMsg.setBornTime(dataObj.getCreationDate().getTime());
									partialMsg.setKey(dataObj.getRefKey());

									codec.encode(partialMsg, out);
								}
							}
						});

						ConsumeMessageCommand cmd = new ConsumeMessageCommand();
						cmd.correlate(req);
						cmd.addMessage(req.getHeader().getCorrelationId(), batch);

						ctx.write(cmd);
					} else {
						try {
							Thread.sleep(1000);
						} catch (InterruptedException e) {
						}
					}
				}
			}
		}.start();
	}

	private ByteBuf stringToByteBuf(String str) {
		return Unpooled.wrappedBuffer(str.getBytes(Charsets.UTF_8));
	}
}
