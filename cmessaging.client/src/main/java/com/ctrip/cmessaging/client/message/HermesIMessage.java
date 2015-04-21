package com.ctrip.cmessaging.client.message;

import java.util.Arrays;

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

	private ConsumerMessage msg;

	public HermesIMessage(ConsumerMessage<byte[]> cmsg, boolean isAutoAck) {
		this.subject = cmsg.getTopic();
		this.exchangeName = MessageUtil.getExchangeName(cmsg);
		this.messageId = MessageUtil.getMessageId(cmsg);
		this.header = MessageUtil.getHeader(cmsg);
		this.body = cmsg.getBody();

		if (isAutoAck) {
			this.ackMode = AckMode.Ack;
		} else {
			this.ackMode = null;
		}

		this.msg = cmsg;
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
		if (ackMode == AckMode.Nack) {
			msg.nack();
		} else if (ackMode == AckMode.Ack) {
			msg.ack();
		}
	}

	@Override
	public String toString() {
		return "HermesIMessage{" +
				  "\n\tsubject='" + subject + '\'' +
				  ", \n\texchangeName='" + exchangeName + '\'' +
				  ", \n\tmessageId='" + messageId + '\'' +
				  ", \n\theader='" + header + '\'' +
				  ", \n\tbody=" + Arrays.toString(body) +
				  ", \n\tackMode=" + ackMode +
				  '}';
	}
}
