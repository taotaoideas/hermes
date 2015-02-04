package com.ctrip.hermes.remoting;

public interface CommandRegistry {

	public void registerProcessor(CommandType type, CommandProcessor processor);

	public CommandProcessor findProcessor(CommandType type);

}
