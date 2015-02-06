package com.ctrip.hermes.container.scanner;

import java.util.List;

import com.ctrip.hermes.container.Subscriber;

public interface Scanner {

	public List<Subscriber> scan();

}
