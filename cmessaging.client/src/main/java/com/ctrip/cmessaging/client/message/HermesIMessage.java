package com.ctrip.cmessaging.client.message;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import com.ctrip.cmessaging.client.IMessage;
import com.ctrip.cmessaging.client.content.AckMode;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class HermesIMessage implements IMessage {

	private String subject;

	private String exchangeName;

	private String messageId;

	private String header;

	private byte[] body;

	private AckMode ackMode;

	public HermesIMessage(ConsumerMessage<byte[]> cmsg) {
		this.subject = cmsg.getTopic();
		this.exchangeName = "Hermes_ExchangeName";
		this.messageId = "Hermes_messageId";
		Map<String, String> properties = new HashMap<>();
		for (Iterator<String> iter = cmsg.getPropertyNames(); iter.hasNext();) {
			String name = iter.next();
			properties.put(name, cmsg.getProperty(name));
		}
		this.header = properties.toString();
		this.body = cmsg.getBody();
		this.ackMode = AckMode.Ack;
	}

	@Override
	public String getSubject() {
		return subject;
	}

	@Override
	public String getExchangeName() {
		return exchangeName;
	}

	@Override
	public String getMessageID() {
		return messageId;
	}

	@Override
	public String getHeader() {
		return header;
	}

	@Override
	public byte[] getBody() {
		return this.body;
	}

	@Override
	public void setAcks(AckMode ackMode) {
		this.ackMode = ackMode;
	}

	@Override
	public AckMode getAcks() {
		return ackMode;
	}

	@Override
	public void dispose() {
		// todo: dispose...
	}
}
