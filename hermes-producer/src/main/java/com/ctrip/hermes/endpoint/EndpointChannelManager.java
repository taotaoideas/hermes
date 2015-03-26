package com.ctrip.hermes.endpoint;

import com.ctrip.hermes.meta.entity.Endpoint;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public interface EndpointChannelManager {

   EndpointChannel getChannel(Endpoint endpoint);

}
