package com.ctrip.hermes.core.meta.internal;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.meta.MetaManager;
import com.ctrip.hermes.core.meta.MetaService;

@Named(type = MetaService.class)
public class DefaultMetaService extends AbstractMetaService  {

	@Inject(ClientMetaManager.ID)
	private MetaManager m_manager;

	private ScheduledExecutorService executor;

	private static final int REFRESH_PERIOD_MINUTES = 1;

	@Override
	public void initialize() throws InitializationException {
		m_meta = m_manager.getMeta();
		executor = Executors.newSingleThreadScheduledExecutor();
		executor.scheduleAtFixedRate(new Runnable() {

			@Override
			public void run() {
				m_meta = m_manager.getMeta();
			}

		}, REFRESH_PERIOD_MINUTES, REFRESH_PERIOD_MINUTES, TimeUnit.MINUTES);
	}

}
