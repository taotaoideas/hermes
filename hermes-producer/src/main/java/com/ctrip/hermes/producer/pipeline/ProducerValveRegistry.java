package com.ctrip.hermes.producer.pipeline;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.pipeline.spi.internel.TracingMessageValve;

public class ProducerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(TracingMessageValve.ID, 0);
	}

}
