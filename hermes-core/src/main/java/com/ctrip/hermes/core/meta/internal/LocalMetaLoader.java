package com.ctrip.hermes.core.meta.internal;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;

import org.unidal.lookup.annotation.Named;
import org.xml.sax.SAXException;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;
import com.ctrip.hermes.meta.transform.DefaultSaxParser;
import com.google.common.io.Files;

@Named(type = MetaLoader.class, value = LocalMetaLoader.ID)
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

	@Override
	public boolean save(Meta meta) {
		try {
			URL resource = getClass().getClassLoader().getResource(PATH);
			byte[] jsonBytes = JSON.toJSONBytes(meta);
			Files.write(jsonBytes, new File(resource.getFile()));
		} catch (IOException e) {
			throw new RuntimeException(String.format("Error save local meta file %s", PATH), e);
		}
		return true;
	}
}
