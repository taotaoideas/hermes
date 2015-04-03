package com.ctrip.cmessaging.client.producer;

import java.lang.Override;
import java.lang.String;
import com.ctrip.cmessaging.client.IProducer;
import com.ctrip.cmessaging.client.exception.IllegalSubject;
import com.ctrip.cmessaging.client.impl.MessageHeader;
import com.ctrip.hermes.producer.api.Producer;

public class HermesProducer implements IProducer {

	@Override
	public void PublishAsync(String content, String subject) throws IllegalSubject {
		PublishAsync(content, subject, new MessageHeader());
	}

	@Override
	public void PublishAsync(String content, String subject, MessageHeader map) throws IllegalSubject {
		/*
		MessageHeader including the info like appid, correlationID, userHeaders...
		Here we didn't write them into message, 'cus these info will be added in the
		following progress.
		 */
		Producer p = Producer.getInstance();

		p.message(subject, content).send();
	}
}
