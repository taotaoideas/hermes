package com.ctrip.hermes.storage.storage.memory;

import com.ctrip.hermes.storage.spi.typed.OffsetStorage;
import com.ctrip.hermes.storage.storage.Offset;

public class MemoryOffsetStorage extends AbstractMemoryStorage<Offset> implements OffsetStorage{

	public MemoryOffsetStorage(String id) {
		super(id);
	}

	@Override
	protected Offset clone(Offset input) {
		return new Offset(input.getId(), input.getOffset());
	}

}
