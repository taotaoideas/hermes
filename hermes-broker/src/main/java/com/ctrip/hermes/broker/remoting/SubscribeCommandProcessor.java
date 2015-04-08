package com.ctrip.hermes.broker.remoting;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.Arrays;
import java.util.List;

import org.unidal.dal.jdbc.DalException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.dal.hermes.MessagePriority;
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
				try {
					doRun();
				} catch (DalException e) {
					e.printStackTrace();
				}
			}

			private void doRun() throws DalException {
				long startMsgId = 0;
				while (true) {
					// TODO only support priority 1
					final List<MessagePriority> dataObjs = reader.read(1, startMsgId, 10);

					if (dataObjs != null && !dataObjs.isEmpty()) {
						startMsgId = dataObjs.get(dataObjs.size() - 1).getId();

						final ConsumerMessageBatch batch = new ConsumerMessageBatch();
						for (MessagePriority dataObj : dataObjs) {
							batch.addMsgSeq(dataObj.getId());
						}
						batch.setTopic(req.getTopic());
						batch.setTransferCallback(new TransferCallback() {

							@Override
							public void transfer(ByteBuf out) {
								for (MessagePriority dataObj : dataObjs) {

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
