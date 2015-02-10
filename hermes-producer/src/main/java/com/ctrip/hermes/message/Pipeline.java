package com.ctrip.hermes.message;

public interface Pipeline<T> {

	public void put(T message);
}
