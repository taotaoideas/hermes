package com.ctrip.hermes.pipeline;

import java.util.List;

import com.ctrip.hermes.pipeline.spi.Valve;

public interface ValveRegistry {
	public void register(Valve valve, String name, int order);

	public List<Valve> getValveList();

}
