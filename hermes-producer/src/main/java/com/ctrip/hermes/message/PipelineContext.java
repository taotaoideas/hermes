package com.ctrip.hermes.message;

public interface PipelineContext<O> {

	public <T> T getSource();

	public void next(Object payload);

	public void put(String name, Object value);

	public <T> T get(String name);
	
	public O getResult();

}
