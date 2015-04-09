package com.ctrip.hermes.core.meta.internal;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.StringWriter;
import java.net.MalformedURLException;
import java.net.URL;

import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.entity.Meta;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	public static final String ID = "remote-meta-loader";

	@Override
	public Meta load() {
		Meta meta = null;
		try {
			// TODO meta server URL
			InputStream is = new URL("http://0.0.0.0:8080/meta").openStream();
			String content = toString(is);
			meta = JSON.parseObject(content, Meta.class);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return meta;
	}

	private static String toString(InputStream is) throws IOException {
		StringWriter writer = new StringWriter();
		InputStreamReader reader = new InputStreamReader(is);
		int bufferSize = 2 * 8192;
		char buffer[] = new char[bufferSize];
		int len = bufferSize;

		while (true) {
			len = reader.read(buffer, 0, bufferSize);
			if (len == -1)
				break;
			writer.write(buffer, 0, len);
		}
		return writer.toString();
	}
}
