package com.ctrip.hermes.remoting.command;

import java.util.List;

public interface CommandProcessor {

	public List<CommandType> commandTypes(); 
	
	public void process(CommandContext ctx);
	
}
