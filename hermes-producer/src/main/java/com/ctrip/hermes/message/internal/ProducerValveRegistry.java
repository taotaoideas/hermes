package com.ctrip.hermes.message.internal;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.spi.internal.TracingMessageValve;

public class ProducerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(TracingMessageValve.ID, 0);
	}

}
