package com.ctrip.hermes.message;

public interface PipelineContext {

	public <T> T getSource();

	public void next(Object payload);

	public void put(String name, String value);

	public <T> T get(String name);

}
