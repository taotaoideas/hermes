package com.ctrip.hermes.example;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.ctrip.hermes.core.message.BrokerConsumerMessage;
import com.ctrip.hermes.core.message.DecodedMessage;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.engine.Subscriber;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class LocalConsumerBootstrap {

	public static void main(String[] args) {
//		Map<Tpp, MessageRawDataBatch> decodedBatches = new HashMap<>();
//
//		for (Map.Entry<Tpp, MessageRawDataBatch> entry : decodedBatches.entrySet()) {
//			MessageRawDataBatch batch = entry.getValue();
//			List<DecodedMessage> msgs = batch.getMessages();
//			
//			Subscriber s = findSubscriber(entry.getKey(), correlationId);
//			
//			List cmsgs = new ArrayList<>();
//			for (DecodedMessage msg : msgs) {
//				BrokerConsumerMessage<Object> bmsg = new BrokerConsumerMessage<>();
//				bmsg.setBody(decode(s.getMessageClass(), msg.readBody()));
//				bmsg.setKey(msg.getKey());
//				//...
//				cmsgs.add(bmsg);
//			}
//			s.getConsumer().consume(cmsgs);
		}
	}

}
