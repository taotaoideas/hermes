package com.ctrip.hermes.local.resource;

import java.util.Date;
import java.util.List;

import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.MediaType;

import com.ctrip.hermes.local.pojo.Order;
import com.ctrip.hermes.local.pojo.OutputMessage;
import com.ctrip.hermes.producer.Producer;
import com.google.common.collect.ArrayListMultimap;


@Path("/producer")
public class ProducerResource {
    static ArrayListMultimap<String/* topic */, OutputMessage> history = ArrayListMultimap.create();

    @GET
    @Path("/send")
    @Produces(MediaType.APPLICATION_JSON)
    public Boolean sendMsg(@QueryParam("topic") String topic,
                        @QueryParam("msg") String msg,
                        @QueryParam("key") String key) {

        Object toSend = msg;
        if("order.new".equals(topic)) {
            String[] parts = msg.split("\\s");
            toSend = new Order(parts[0], parts.length < 2 ? 0 : Double.parseDouble(parts[1]));
        }

        Boolean isSuccess = true;
        if (null == key) {
            Producer.getInstance().message(topic, toSend).send();
            history.put(topic, new OutputMessage(msg, key, null, null, null, new Date().getTime()));
        } else {
            Producer.getInstance().message(topic, toSend).withKey(key).send();
            history.put(topic, new OutputMessage(msg, key, null, null, null, new Date().getTime()));
        }
        return isSuccess;
    }

    @GET
    @Path("/history")
    @Produces(MediaType.APPLICATION_JSON)
    public List<OutputMessage> sendMsg(@QueryParam("topic") String topic) {
        return history.get(topic);
    }
}
