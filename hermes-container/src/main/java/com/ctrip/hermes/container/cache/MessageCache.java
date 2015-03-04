package com.ctrip.hermes.container.cache;

import java.util.List;

import com.ctrip.hermes.storage.message.Record;

public interface MessageCache {

	public void addMessages(List<Record> msgs);
	
	
	
}
