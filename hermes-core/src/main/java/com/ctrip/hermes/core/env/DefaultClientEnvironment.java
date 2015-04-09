package com.ctrip.hermes.core.env;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Named;

@Named(type = ClientEnvironment.class)
public class DefaultClientEnvironment implements ClientEnvironment, Initializable {
	private final static String PRODUCER_DEFAULT_FILE = "/hermes-producer.properties";

	private final static String PRODUCER_PATTERN = "/hermes-producer-%s.properties";

	private ConcurrentMap<String, Properties> m_producerCache = new ConcurrentHashMap<>();

	private Properties m_producerDefault;

	@Override
	public Properties getProducerConfig(String topic) throws IOException {
		Properties properties = m_producerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(PRODUCER_PATTERN, topic), m_producerDefault);
			m_producerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}

	private Properties readConfigFile(String configPath) throws IOException {
		return readConfigFile(configPath, null);
	}

	private Properties readConfigFile(String configPath, Properties defaults) throws IOException {
		InputStream in = this.getClass().getResourceAsStream(configPath);
		Properties props = new Properties(defaults);

		if (in != null) {
			props.load(in);
		}

		return props;
	}

	@Override
	public void initialize() throws InitializationException {
		try {
			m_producerDefault = readConfigFile(PRODUCER_DEFAULT_FILE);
		} catch (IOException e) {
			throw new InitializationException("Error read producer default config file", e);
		}
	}

}
