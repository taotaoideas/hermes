package com.ctrip.hermes.storage.storage.memory;

import com.ctrip.hermes.storage.message.Resend;
import com.ctrip.hermes.storage.spi.typed.ResendStorage;

public class MemoryResendStorage extends AbstractMemoryStorage<Resend> implements ResendStorage{

	public MemoryResendStorage(String id) {
		super(id);
	}

	@Override
	protected Resend clone(Resend input) {
		Resend newResend = new Resend(input.getRange(), input.getDue());
		newResend.setOffset(input.getOffset());

		return newResend;
	}

}
