package com.ctrip.hermes.container;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;

import com.ctrip.hermes.engine.DecodeMessageValve;
import com.ctrip.hermes.message.internal.AbstractValveRegistry;

public class ConsumerValveRegistry extends AbstractValveRegistry implements Initializable {

	@Override
	public void initialize() throws InitializationException {
		doRegister(DecodeMessageValve.ID, 1);
	}

}
