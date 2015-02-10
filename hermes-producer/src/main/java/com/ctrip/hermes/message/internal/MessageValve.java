package com.ctrip.hermes.message.internal;

import com.ctrip.hermes.message.Message;
import com.ctrip.hermes.spi.Valve;

public interface MessageValve extends Valve<Message<Object>> {

}
