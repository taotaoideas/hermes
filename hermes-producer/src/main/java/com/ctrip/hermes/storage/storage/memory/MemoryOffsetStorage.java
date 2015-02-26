package com.ctrip.hermes.storage.storage.memory;

import com.ctrip.hermes.storage.storage.Offset;

public class MemoryOffsetStorage extends AbstractMemoryStorage<Offset> {

	public MemoryOffsetStorage(String id) {
		super(id);
	}

	@Override
	protected Offset clone(Offset input) {
		return new Offset(input.getId(), input.getOffset());
	}

}
