package com.ctrip.hermes.broker.transport.command.processor;

import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.queue.MessageQueueManager;
import com.ctrip.hermes.core.bo.Tpp;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

public class SendMessageCommandProcessor implements CommandProcessor {

	@Inject
	private MessageQueueManager m_queueManager;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(final CommandProcessorContext ctx) {
		SendMessageCommand reqCmd = (SendMessageCommand) ctx.getCommand();

		Map<Tpp, MessageRawDataBatch> rawBatches = reqCmd.getMessageRawDataBatches();

		final SendMessageAckCommand ack = new SendMessageAckCommand(reqCmd.getMessageCount());
		ack.correlate(reqCmd);

		FutureCallback<Map<Integer, Boolean>> completionCallback = new AppendMessageCompletionCallback(ack, ctx);

		for (Map.Entry<Tpp, MessageRawDataBatch> entry : rawBatches.entrySet()) {
			MessageRawDataBatch batch = entry.getValue();
			Tpp tpp = entry.getKey();
			try {
				ListenableFuture<Map<Integer, Boolean>> future = m_queueManager.appendMessageAsync(tpp, batch);

				Futures.addCallback(future, completionCallback);

			} catch (Exception e) {
				// TODO
				e.printStackTrace();
			}
		}

	}

	private static class AppendMessageCompletionCallback implements FutureCallback<Map<Integer, Boolean>> {
		private SendMessageAckCommand m_ack;

		private CommandProcessorContext m_ctx;

		private AtomicBoolean m_written = new AtomicBoolean(false);

		public AppendMessageCompletionCallback(SendMessageAckCommand ack, CommandProcessorContext ctx) {
			m_ack = ack;
			m_ctx = ctx;
		}

		@Override
		public void onSuccess(Map<Integer, Boolean> results) {
			m_ack.addResults(results);

			if (m_ack.isAllResultsSet()) {
				if (m_written.compareAndSet(false, true)) {
					m_ctx.write(m_ack);
				}
			}

		}

		@Override
		public void onFailure(Throwable t) {
			// TODO
		}
	}
}
