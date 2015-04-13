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
	
	private final static String CONSUMER_DEFAULT_FILE = "/hermes-consumer.properties";
	
	private final static String CONSUMER_PATTERN = "/hermes-consumer-%s.properties";
	
	private final static String GLOBAL_DEFAULT_FILE = "/hermes.properties";
	
	private ConcurrentMap<String, Properties> m_producerCache = new ConcurrentHashMap<>();

	private ConcurrentMap<String, Properties> m_consumerCache = new ConcurrentHashMap<>();
	
	private Properties m_producerDefault;

	private Properties m_consumerDefault;

	private Properties m_globalDefault;
	
	@Override
	public Properties getProducerConfig(String topic) throws IOException {
		Properties properties = m_producerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(PRODUCER_PATTERN, topic), m_producerDefault);
			m_producerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}

	@Override
	public Properties getConsumerConfig(String topic) throws IOException {
		Properties properties = m_consumerCache.get(topic);
		if (properties == null) {
			properties = readConfigFile(String.format(CONSUMER_PATTERN, topic), m_consumerDefault);
			m_consumerCache.putIfAbsent(topic, properties);
		}

		return properties;
	}
	
	@Override
	public Properties getGlobalConfig() throws IOException {
		return m_globalDefault;
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
			m_consumerDefault = readConfigFile(CONSUMER_DEFAULT_FILE);
			m_globalDefault = readConfigFile(GLOBAL_DEFAULT_FILE);
		} catch (IOException e) {
			throw new InitializationException("Error read producer default config file", e);
		}
	}

}
