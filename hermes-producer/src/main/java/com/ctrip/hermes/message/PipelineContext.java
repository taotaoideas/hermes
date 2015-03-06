package com.ctrip.hermes.message;

public interface PipelineContext<O> {

	public <T> T getSource();

	public O next(Object payload);

	public void put(String name, String value);

	public <T> T get(String name);

}
