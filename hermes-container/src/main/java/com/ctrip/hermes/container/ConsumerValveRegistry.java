package com.ctrip.hermes.container;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.engine.DecodeMessageValve;

public class ConsumerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(DecodeMessageValve.ID, 1);
	}

}
