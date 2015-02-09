package com.ctrip.hermes.container;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

public class ContainerTest extends ComponentTestCase {

	@Test
	public void submitConsumer() {
		Container c = lookup(Container.class);
		
		c.start();
	}

}
