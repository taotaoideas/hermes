package com.ctrip.hermes.core.env;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.unidal.lookup.annotation.Named;

@Named(type = ClientEnvironment.class)
public class DefaultClientEnvironment implements ClientEnvironment {
	private final static String PRODUCER_PATTERN = "/hermes-producer-%s.properties";

	private ConcurrentMap<String, Properties> m_producerCache = new ConcurrentHashMap<>();

	@Override
	public Properties getProducerConfig(String topic) throws IOException {
		Properties properties = m_producerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(PRODUCER_PATTERN, topic));
			m_producerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}

	private Properties readConfigFile(String configPath) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(configPath);
		Properties props = new Properties();

		if (in != null) {
			props.load(in);
		}

		return props;
	}

}
