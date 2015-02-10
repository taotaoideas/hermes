package com.ctrip.hermes.message;

import java.util.List;

import com.ctrip.hermes.spi.Valve;

public interface ValveRegistry<T> {
	public void registerValve(Valve<T> valve, String name, int order);

	public List<Valve<T>> getValveList();

}
