package com.ctrip.hermes.remoting;

public interface CommandCodec {

	public byte[] encode(Command cmd);

	public Command decode(byte[] bytes);

}
