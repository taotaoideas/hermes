package com.ctrip.hermes.endpoint;

import com.ctrip.hermes.remoting.command.Command;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannel {

	void writeCommand(Command command);

	void start();

}
