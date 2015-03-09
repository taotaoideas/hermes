package com.ctrip.hermes.remoting.future;

import com.google.common.util.concurrent.SettableFuture;

public interface FutureManager {

	<T> SettableFuture<T> newFuture(int correlationId);

	void futureDone(int correlationId, Object result);

}
