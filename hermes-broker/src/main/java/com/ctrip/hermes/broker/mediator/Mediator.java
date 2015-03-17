package com.ctrip.hermes.broker.mediator;

import java.io.IOException;

public interface Mediator {

	public long[] claim(int batchSize);
	
	public void close() throws IOException;

}
