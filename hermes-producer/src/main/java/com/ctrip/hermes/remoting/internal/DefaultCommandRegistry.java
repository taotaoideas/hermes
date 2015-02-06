package com.ctrip.hermes.remoting.internal;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.ContainerHolder;

import com.ctrip.hermes.remoting.CommandProcessor;
import com.ctrip.hermes.remoting.CommandRegistry;
import com.ctrip.hermes.remoting.CommandType;

public class DefaultCommandRegistry extends ContainerHolder implements Initializable, CommandRegistry {

	private Map<CommandType, CommandProcessor> m_processors = new ConcurrentHashMap<CommandType, CommandProcessor>();

	@Override
	public void registerProcessor(CommandType type, CommandProcessor processor) {
		if (m_processors.containsKey(type)) {
			throw new IllegalArgumentException(String.format("Command processor for type %s is already registered", type));
		}

		m_processors.put(type, processor);
	}

	@Override
	public CommandProcessor findProcessor(CommandType type) {
		return m_processors.get(type);
	}

	@Override
	public void initialize() throws InitializationException {
		List<CommandProcessor> processors = lookupList(CommandProcessor.class);

		for (CommandProcessor p : processors) {
			for (CommandType type : p.commandTypes()) {
				registerProcessor(type, p);
			}
		}
	}

}
