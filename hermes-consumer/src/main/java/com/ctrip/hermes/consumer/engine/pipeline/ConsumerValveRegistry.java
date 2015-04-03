package com.ctrip.hermes.consumer.engine.pipeline;

import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.pipeline.AbstractValveRegistry;
import com.ctrip.hermes.core.pipeline.ValveRegistry;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
@Named(type = ValveRegistry.class, value = ConsumerValveRegistry.ID)
public class ConsumerValveRegistry extends AbstractValveRegistry {

	public static final String ID = "consumer";
}
