package com.ctrip.hermes.core.endpoint;

import com.ctrip.hermes.core.transport.command.Command;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannel {

	void writeCommand(Command command);

	void start();

}
