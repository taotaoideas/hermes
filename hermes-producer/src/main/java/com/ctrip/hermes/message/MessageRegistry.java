package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.MessageValve;

public interface MessageRegistry {
	public void registerValve(String name, int order);

	public List<MessageValve> getValveList();
	
}
