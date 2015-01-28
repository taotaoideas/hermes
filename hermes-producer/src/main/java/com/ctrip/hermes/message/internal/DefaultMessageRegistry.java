package com.ctrip.hermes.message.internal;

import java.util.ArrayList;
import java.util.List;

import com.ctrip.hermes.message.MessageRegistry;
import com.ctrip.hermes.spi.MessageValve;

public class DefaultMessageRegistry implements MessageRegistry {
	@Override
	public void registerValve(String name, int order) {

	}

	@Override
	public List<MessageValve> getValveList() {
		List<MessageValve> list = new ArrayList<MessageValve>();

		// TODO
		return list;
	}
}
