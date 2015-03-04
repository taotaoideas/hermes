package com.ctrip.hermes.localDev.pojo;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.storage.message.Record;
import com.ctrip.hermes.storage.storage.Offset;

public class OutputMessage {

	public String message;

	public String key;

	public Offset offset;

	public Offset ackOffset;

	public Map<String, String> properties = new HashMap<String, String>();

	public long timestamp;

	public OutputMessage(String message, String key, Offset offset, Offset ackOffset, Map<String, String> properties,
	      long timestamp) {
		this.message = message;
		this.key = key;
		this.offset = offset;
		this.ackOffset = ackOffset;
		this.properties = properties;
		this.timestamp = timestamp;
	}

	public static OutputMessage convert(Record msg) {
		String body = null, key = null;
		body = new String(msg.getContent());
		key = msg.getKey();

		// todo: get timestamp from <Message>msg

		return new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), msg.getProperties(),
		      new Date().getTime());
	}

	public static OutputMessage convert(StoredMessage<byte[]> msg) {
		String body = null, key = null;
		body = new String(msg.getBody());
		key = msg.getKey();

		// todo: get timestamp from <Message>msg

		return new OutputMessage(body, key, msg.getOffset(), msg.getAckOffset(), new HashMap<String, String>(),
		      new Date().getTime());
	}
}