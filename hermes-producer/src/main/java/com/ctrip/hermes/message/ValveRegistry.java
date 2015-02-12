package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.Valve;

public interface ValveRegistry {
	public void registerValve(Valve valve, String name, int order);

	public List<Valve> getValveList();

}
