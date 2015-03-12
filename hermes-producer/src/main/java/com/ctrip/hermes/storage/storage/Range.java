package com.ctrip.hermes.storage.storage;

public interface Range {

	void setId(String id);

	String getId();

	void setStartOffset(Offset offset);

	Offset getStartOffset();

	void setEndOffset(Offset offset);

	Offset getEndOffset();

}
