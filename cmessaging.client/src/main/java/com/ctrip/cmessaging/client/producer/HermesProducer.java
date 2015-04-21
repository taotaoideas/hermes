package com.ctrip.cmessaging.client.producer;

import java.util.UUID;

import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.constant.AdapterConstant;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.impl.MessageHeader;
import com.ctrip.cmessaging.client.message.MessageUtil;
import com.ctrip.hermes.producer.api.Producer;

public class HermesProducer implements IProducer {

	private String exchangeName;
	private String appId;

	public HermesProducer(String exchangeName, String appId) {
		this.exchangeName = exchangeName;
		this.appId = appId;
	}

	@Override
	public void PublishAsync(String content, String subject) throws IllegalSubject {
		PublishAsync(content, subject, new MessageHeader());
	}

	@Override
	public void PublishAsync(String content, String subject, MessageHeader map) throws IllegalSubject {
		Producer p = Producer.getInstance();

		// as cmessage-java-client: SimpleProducer generated messaged id.
		String messageId = UUID.randomUUID().toString();

		p.message(subject, content)
				  .addProperty(AdapterConstant.CMESSAGING_EXCHANGENAME, exchangeName)
				  .addProperty(AdapterConstant.CMESSAGING_MESSAGEID, messageId)
				  .addProperty(AdapterConstant.CMESSAGING_HEADER,
							 MessageUtil.buildHeader(exchangeName, messageId, subject, content, map))
				  .send();
	}
}
