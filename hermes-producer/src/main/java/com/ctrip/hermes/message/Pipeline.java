package com.ctrip.hermes.message;

public interface Pipeline<O> {

	public O put(Object msg);
}
