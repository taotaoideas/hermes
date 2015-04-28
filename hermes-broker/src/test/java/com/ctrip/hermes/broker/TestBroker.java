package com.ctrip.hermes.broker;

import org.junit.BeforeClass;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;

public class TestBroker extends ComponentTestCase {

	@BeforeClass
	public static void beforeClass() {
		System.setProperty("devMode", "true");
	}

	@Test
	public void test() throws Exception {
		lookup(BrokerBootstrap.class).start();
	}

}
