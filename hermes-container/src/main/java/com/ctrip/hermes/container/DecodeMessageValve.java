package com.ctrip.hermes.container;

import com.ctrip.hermes.message.PipelineContext;
import com.ctrip.hermes.spi.Valve;

public class DecodeMessageValve implements Valve {

	public static final String ID = "decode-message";

	@Override
	public void handle(PipelineContext ctx, Object payload) {
		byte[] body = (byte[]) payload;

		// TODO
		ctx.next(new String(body));
	}

}
