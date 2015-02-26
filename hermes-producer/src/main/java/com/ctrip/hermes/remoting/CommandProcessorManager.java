package com.ctrip.hermes.remoting;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.logging.LogEnabled;
import org.codehaus.plexus.logging.Logger;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

public class CommandProcessorManager implements Initializable, LogEnabled {

	@Inject
	private CommandRegistry m_registry;

	private ExecutorService m_executor;

	private Logger m_logger;

	private void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();
		CommandProcessor processor = m_registry.findProcessor(cmd.getType());

		if (processor == null) {
			m_logger.error(String.format("Command processor for type %s is not found", cmd.getType()));
		} else {
			try {
				processor.process(ctx);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void offer(final CommandContext ctx) {
		m_executor.submit(new Runnable() {

			@Override
			public void run() {
				process(ctx);
			}
		});
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		BlockingQueue<Runnable> workQueue = new LinkedBlockingQueue<Runnable>();
		m_executor = new ThreadPoolExecutor(10, 10, Integer.MAX_VALUE, TimeUnit.SECONDS, workQueue);
	}

	@Override
	public void enableLogging(Logger logger) {
		m_logger = logger;
	}
}
