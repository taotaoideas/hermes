package com.ctrip.hermes.broker.mediator;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.recipes.shared.SharedValue;
import org.apache.curator.framework.recipes.shared.VersionedValue;

public class ZKMediator implements Mediator {

	private SharedValue m_shared;

	public ZKMediator(CuratorFramework client, String path) throws Exception {
		byte[] seedValue = toBytes(0L);
		m_shared = new SharedValue(client, path, seedValue);
		m_shared.start();
	}

	@Override
	public long[] claim(int batchSize) {
		while (true) {
			VersionedValue<byte[]> startVersion = m_shared.getVersionedValue();
			long start = fromBytes(startVersion.getValue());
			try {
				long end = start + batchSize;
				if (m_shared.trySetValue(startVersion, toBytes(end))) {
					return new long[] { start, end };
				}
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
			}
		}
	}

	// TODO do not use heavy weight ByteBuffer
	private byte[] toBytes(long value) {
		byte[] bytes = new byte[8];
		ByteBuffer.wrap(bytes).putLong(value);
		return bytes;
	}

	private long fromBytes(byte[] bytes) {
		return ByteBuffer.wrap(bytes).getLong();
	}

	@Override
	public void close() throws IOException {
		m_shared.close();
	}

}
