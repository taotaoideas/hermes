package com.ctrip.hermes.remoting.future;

import com.google.common.util.concurrent.SettableFuture;

public interface FutureManager {

	<T> SettableFuture<T> newFuture(long correlationId);

	void futureDone(long correlationId, Object result);

}
