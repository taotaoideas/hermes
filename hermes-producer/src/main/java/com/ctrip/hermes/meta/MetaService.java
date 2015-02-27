package com.ctrip.hermes.meta;

import com.ctrip.hermes.meta.entity.Connector;
import com.ctrip.hermes.meta.entity.Storage;

public interface MetaService {

	Connector getConnector(String topic);

	Storage getStorage(String topic);
}
