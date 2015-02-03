package com.ctrip.hermes.meta;

public interface MetaService {

	String getConnectorType(String topic);

	String getStorageType(String topic);

}
