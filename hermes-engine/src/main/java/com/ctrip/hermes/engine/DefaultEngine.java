package com.ctrip.hermes.engine;

import java.util.List;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.engine.scanner.Scanner;

public class DefaultEngine extends ContainerHolder implements Engine {

	@Inject
	private Scanner m_scanner;

	@Inject
	private ConsumerBootstrap m_consumerManager;

	@Override
	public void start() {
		List<Subscriber> subcribers = m_scanner.scan();

		for (Subscriber s : subcribers) {
			m_consumerManager.startConsumer(s);
		}
	}

}
