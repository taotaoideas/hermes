package com.ctrip.hermes.remoting;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

public class CommandProcessorManager implements Initializable {

	@Inject
	private CommandRegistry m_registry;

	private BlockingQueue<CommandContext> m_toProcess = new LinkedBlockingQueue<CommandContext>();

	private void process(CommandContext ctx) {
		Command cmd = ctx.getCommand();
		CommandProcessor processor = m_registry.findProcessor(cmd.getType());

		if (processor == null) {
			System.out.println(String.format("Command processor for type %s is not found", cmd.getType()));
		} else {
			try {
				processor.process(ctx);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}
	}

	public void offer(CommandContext ctx) {
		m_toProcess.offer(ctx);
	}

	@Override
	public void initialize() throws InitializationException {
		// TODO
		new Thread() {

			@Override
			public void run() {
				while (true) {
					try {
						process(m_toProcess.take());
					} catch (InterruptedException e) {
						return;
					}
				}
			}

		}.start();
	}
}
