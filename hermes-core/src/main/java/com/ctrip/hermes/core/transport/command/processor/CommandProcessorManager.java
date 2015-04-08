package com.ctrip.hermes.core.transport.command.processor;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.transport.command.Command;
import com.ctrip.hermes.core.transport.command.CommandType;

@Named(type = CommandProcessorManager.class)
public class CommandProcessorManager implements Initializable, LogEnabled {

	@Inject
	private CommandProcessorRegistry m_registry;

	private Map<CommandType, ExecutorService> m_executors = new ConcurrentHashMap<>();

	private Logger m_logger;

	private void process(CommandProcessorContext ctx) {
		Command cmd = ctx.getCommand();
		CommandType type = cmd.getHeader().getType();
		CommandProcessor processor = m_registry.findProcessor(type);

		if (processor == null) {
			m_logger.error(String.format("Command processor for type %s is not found", type));
		} else {
			try {
				processor.process(ctx);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void offer(final CommandProcessorContext ctx) {
		Command cmd = ctx.getCommand();
		CommandType type = cmd.getHeader().getType();
		ExecutorService executorService = m_executors.get(type);

		if (executorService == null) {
			throw new IllegalArgumentException(String.format("Unknown command type[%s]", type));
		} else {
			executorService.submit(new Runnable() {

				@Override
				public void run() {
					process(ctx);
				}
			});
		}
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		Set<CommandType> commandTypes = m_registry.listAllCommandTypes();

		for (CommandType type : commandTypes) {
			BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
			m_executors.put(type, new ThreadPoolExecutor(10, 10, Integer.MAX_VALUE, TimeUnit.SECONDS, workQueue));
		}

	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}
}
