package com.ctrip.hermes.engine.scanner;

import java.util.List;

import com.ctrip.hermes.engine.Subscriber;

public interface Scanner {

	public List<Subscriber> scan();

}
