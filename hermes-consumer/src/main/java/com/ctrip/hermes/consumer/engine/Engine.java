package com.ctrip.hermes.consumer.engine;

import java.util.List;

public interface Engine {

	public void start(List<Subscriber> subscribers);

}
