package com.ctrip.hermes.meta;

import com.ctrip.hermes.message.codec.internal.CodecType;
import com.ctrip.hermes.message.internal.StorageType;

public class Meta {

	private String m_storageType;

	public void setStorageType(String storageType) {
		m_storageType = storageType;
	}

	public StorageType getStorageType(String topic) {
		return StorageType.valueOf(m_storageType);
	}

	public CodecType getCodecType(String topic) {
		return CodecType.JSON;
	}
}