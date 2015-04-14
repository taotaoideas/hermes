package com.ctrip.hermes.core.message;

import java.util.Map;

public interface ConsumerMessage<T> {

	public static enum MessageStatus {
		SUCCESS, FAIL, NOT_SET;
	}

	public void nack();

	public <V> V getProperty(String name);

	public Map<String, Object> getProperties();

	public long getBornTime();

	public String getTopic();

	public String getKey();

	public T getBody();

	public MessageStatus getStatus();

	public void ack();

}
