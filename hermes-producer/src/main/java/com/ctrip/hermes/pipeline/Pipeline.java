package com.ctrip.hermes.pipeline;

public interface Pipeline<T> {

	public T put(Object msg);
}
