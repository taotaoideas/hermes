package com.ctrip.hermes.core.message;

import java.util.Map;

public interface ConsumerMessage<T> {

	public void nack();

	public <V> V getProperty(String name);

	public Map<String, Object> getProperties();

	public long getBornTime();

	public String getTopic();

	public String getKey();

	public T getBody();

	public boolean isSuccess();

}
