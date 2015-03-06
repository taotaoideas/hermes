package com.ctrip.hermes.storage.storage;

public interface Range extends Locatable {

	void setId(String id);

	String getId();

	void setStartOffset(Offset offset);
	
	Offset getStartOffset();

	void setEndOffset(Offset offset);
	
	Offset getEndOffset();

	boolean contains(Offset o);

}
