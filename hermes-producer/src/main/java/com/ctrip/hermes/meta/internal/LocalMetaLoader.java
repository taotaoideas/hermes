package com.ctrip.hermes.meta.internal;

import java.io.IOException;
import java.io.InputStream;

import org.xml.sax.SAXException;

import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;

public class LocalMetaLoader implements MetaLoader {

	public static final String ID = "local-meta-loader";

	private static final String PATH = "/com/ctrip/hermes/meta/meta-local.xml";

	@Override
	public Meta load() {

		InputStream in = getClass().getResourceAsStream(PATH);

		if (in == null) {
			throw new RuntimeException(String.format("Local meta file %s not found on classpath", PATH));
		} else {
			try {
				return DefaultSaxParser.parse(in);
			} catch (SAXException | IOException e) {
				throw new RuntimeException(String.format("Error parse local meta file %s", PATH), e);
			}
		}
	}
}
