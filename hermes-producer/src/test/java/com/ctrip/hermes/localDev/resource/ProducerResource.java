package com.ctrip.hermes.localDev.resource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import javax.xml.bind.JAXBElement;

import com.ctrip.hermes.localDev.pojo.OutputMessage;
import com.ctrip.hermes.producer.Producer;


@Path("/producer")
public class ProducerResource {
    static List<OutputMessage> history = new ArrayList<>();

    @GET
    @Path("/send")
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean sendMsg(@QueryParam("topic") String topic,
                        @QueryParam("msg") String msg,
                        @QueryParam("key") String key) {
        Boolean isSuccess = true;
        if (null == key) {
            Producer.getInstance().message(topic, msg).send();
            history.add(new OutputMessage(msg, key, null,null, null, new Date().getTime()));
        } else {
            Producer.getInstance().message(topic, msg).withKey(key).send();
            history.add(new OutputMessage(msg, key, null,null, null, new Date().getTime()));
        }
        return isSuccess;
    }

    @GET
    @Path("/history")
    @Produces(MediaType.APPLICATION_JSON)
    public List<OutputMessage> sendMsg(@QueryParam("topic") String topic) {
        return history;
    }
}
