package com.ctrip.hermes.message;

public interface Pipeline {

	public void put(Object msg);
}
