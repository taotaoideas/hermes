package com.ctrip.hermes.producer.pipeline;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.core.pipeline.ValveRegistry;
import com.ctrip.hermes.core.pipeline.spi.internal.TracingMessageValve;

@Named(type = ValveRegistry.class, value = ProducerValveRegistry.ID)
public class ProducerValveRegistry extends AbstractValveRegistry implements Initializable {

	public static final String ID = "producer";

	@Override
	public void initialize() throws InitializationException {
		doRegister(TracingMessageValve.ID, 0);
	}

}
