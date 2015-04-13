package com.ctrip.hermes.core.meta.internal;

import java.io.BufferedWriter;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.net.HttpURLConnection;
import java.net.URL;

import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.meta.entity.Meta;
import com.google.common.io.ByteStreams;

@Named(type = MetaLoader.class, value = RemoteMetaLoader.ID)
public class RemoteMetaLoader implements MetaLoader {

	public static final String ID = "remote-meta-loader";

	@Inject
	private ClientEnvironment m_clientEnvironment;

	@Override
	public Meta load() {
		Meta meta = null;
		try {
			String host = m_clientEnvironment.getGlobalConfig().getProperty("meta-host");
			String port = m_clientEnvironment.getGlobalConfig().getProperty("meta-port");
			URL metaURL = new URL("http://" + host + ":" + port + "/meta");
			InputStream is = metaURL.openStream();
			String jsonString = new String(ByteStreams.toByteArray(is));
			meta = JSON.parseObject(jsonString, Meta.class);
		} catch (Exception e) {
			throw new RuntimeException("Load remote meta failed", e);
		}
		return meta;
	}

	@Override
	public boolean save(Meta meta) {
		try {
			String host = m_clientEnvironment.getGlobalConfig().getProperty("meta-host");
			String port = m_clientEnvironment.getGlobalConfig().getProperty("meta-port");
			URL metaURL = new URL("http://" + host + ":" + port + "/meta");
			HttpURLConnection conn = (HttpURLConnection) metaURL.openConnection();
			conn.setRequestMethod("POST");
			OutputStream os = conn.getOutputStream();
			BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os, "UTF-8"));
			String jsonString = JSON.toJSONString(meta);
			writer.write(jsonString);
			writer.close();
		} catch (Exception e) {
			throw new RuntimeException("Save remote meta failed", e);
		}
		return true;
	}
}
