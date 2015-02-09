package com.ctrip.hermes.remoting.internal;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.remoting.Command;
import com.ctrip.hermes.remoting.CommandCodec;

public class DefaultCommandCodec implements CommandCodec {

	@Override
	public byte[] encode(Command cmd) {
		return JSON.toJSONBytes(cmd);
	}

	@Override
	public Command decode(byte[] bytes) {
		return JSON.parseObject(bytes, Command.class);
	}

}
