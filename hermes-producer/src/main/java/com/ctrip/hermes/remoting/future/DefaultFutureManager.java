package com.ctrip.hermes.remoting.future;

import java.util.HashMap;
import java.util.Map;

import com.google.common.util.concurrent.SettableFuture;

public class DefaultFutureManager implements FutureManager {

	private Map<Long, SettableFuture<Object>> m_futures = new HashMap<>();

	@SuppressWarnings("unchecked")
	@Override
	public <T> SettableFuture<T> newFuture(long correlationId) {
		SettableFuture<Object> f = SettableFuture.create();
		m_futures.put(correlationId, f);

		return (SettableFuture<T>) f;
	}

	@Override
	public void futureDone(long correlationId, Object result) {
		m_futures.get(correlationId).set(result);
		m_futures.remove(correlationId);
	}

}
