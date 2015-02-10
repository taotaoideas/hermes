package com.ctrip.hermes.container;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.engine.Engine;

public class ContainerTest extends ComponentTestCase {

	@Test
	public void submitConsumer() {
		Engine c = lookup(Engine.class);
		
		c.start();
	}

}
