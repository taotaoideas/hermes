package com.ctrip.hermes.container;

import java.util.List;

import org.unidal.lookup.ContainerHolder;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.container.scanner.Scanner;

public class DefaultContainer extends ContainerHolder implements Container {

	@Inject
	private Scanner m_scanner;

	@Override
	public void start() {
		List<Subscriber> subcribers = m_scanner.scan();
	}

}
