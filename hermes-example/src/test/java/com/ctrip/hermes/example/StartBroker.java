package com.ctrip.hermes.example;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;
import org.unidal.lookup.LookupException;

import com.ctrip.hermes.broker.bootstrap.BrokerBootstrap;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class StartBroker extends ComponentTestCase {

	@Test
	public void test() throws Exception {

		Thread t = new Thread() {
			public void run() {

				try {
					lookup(BrokerBootstrap.class).start();
				} catch (LookupException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (Exception e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			};
		};

		t.start();
		System.out.println("Broker started...");
		System.in.read();
	}
}
