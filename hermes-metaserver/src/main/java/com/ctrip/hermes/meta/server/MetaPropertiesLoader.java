package com.ctrip.hermes.meta.server;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class MetaPropertiesLoader {

	private static final String PATH = "/meta.properties";

	public static Properties load() {
		InputStream in = MetaPropertiesLoader.class.getResourceAsStream(PATH);
		Properties prop = new Properties();
		if (in == null) {
			throw new RuntimeException(String.format("Local properties file %s not found on classpath", PATH));
		} else {
			try {
	         prop.load(in);
         } catch (IOException e) {
	         e.printStackTrace();
         }
		}
		return prop;
	}
}
