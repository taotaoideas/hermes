package com.ctrip.hermes.message;

import java.util.Map;

public interface Message<T> {

	public abstract void nack();

	public abstract <V> V getProperty(String name);

	public abstract Map<String, Object> getProperties();

	public abstract long getBornTime();

	public abstract String getPartition();

	public abstract String getTopic();

	public abstract String getKey();

	public abstract T getBody();

	public abstract boolean isSuccess();

}
