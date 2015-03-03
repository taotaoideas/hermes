package com.ctrip.hermes.localDev.resource;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import com.ctrip.hermes.producer.Producer;


@Path("/producer")
public class ProducerResource {

    @GET
    @Path("/send")
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean sendMsg(@QueryParam("topic") String topic,
                        @QueryParam("msg") String msg,
                        @QueryParam("key") String key) {
        Boolean isSuccess = true;
        if (null == key) {
            Producer.getInstance().message(topic, msg).send();
        } else {
            Producer.getInstance().message(topic, msg).withKey(key).send();
        }
        return isSuccess;
    }
}
