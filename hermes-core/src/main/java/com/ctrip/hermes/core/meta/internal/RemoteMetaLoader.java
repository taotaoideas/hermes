package com.ctrip.hermes.core.meta.internal;

import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;

import org.mortbay.util.IO;
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
			String content = IO.toString(is);
			meta = JSON.parseObject(content, Meta.class);
		} catch (MalformedURLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			e.printStackTrace();
		}
		return meta;
	}

}
