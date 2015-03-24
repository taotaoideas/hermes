package com.ctrip.hermes.channel;

import java.util.concurrent.Future;

import com.ctrip.hermes.remoting.command.Command;

public interface ProducerChannel {

	public Future<SendResult> send(Command command);

	public void close();

}
