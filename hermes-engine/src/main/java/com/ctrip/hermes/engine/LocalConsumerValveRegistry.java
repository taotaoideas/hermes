package com.ctrip.hermes.engine;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;

public class LocalConsumerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(DecodeMessageValve.ID, 0);
	}

}
