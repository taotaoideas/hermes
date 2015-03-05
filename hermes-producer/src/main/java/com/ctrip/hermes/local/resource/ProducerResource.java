package com.ctrip.hermes.local.resource;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.local.pojo.OutputMessage;
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
