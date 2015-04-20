package com.ctrip.hermes.meta.rest;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.UUID;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.Invocation.Builder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.GenericType;
import javax.ws.rs.core.Response;
import javax.ws.rs.core.Response.Status;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.alibaba.fastjson.JSON;
import com.ctrip.hermes.meta.pojo.TopicView;
import com.ctrip.hermes.meta.server.MetaRestServer;
import com.google.common.base.Charsets;
import com.google.common.io.Files;

public class TopicServerTest extends ComponentTestCase {

  private MetaRestServer server;

  @Before
  public void startServer() {
    server = lookup(MetaRestServer.class);
    server.start();
  }

  @After
  public void stopServer() {
    server.stop();
  }

  @Test
  public void testGetTopic() {
    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(StandaloneRestServer.HOST);
    String topic = "kafka.SimpleTopic";
    Builder request = webTarget.path("topics/" + topic).request();
    TopicView actual = request.get(TopicView.class);
    System.out.println(actual);
    Assert.assertEquals(topic, actual.getName());
  }

  @Test
  public void testListTopic() {
    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(StandaloneRestServer.HOST);
    Builder request = webTarget.path("topics/").queryParam("pattern", ".*").request();
    Response response = request.get();
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
    List<TopicView> topics = response.readEntity(new GenericType<List<TopicView>>() {});
    System.out.println(topics);
    Assert.assertTrue(topics.size() > 0);
  }

  @Test
  public void testCreateExitingTopic() throws IOException {
    String jsonString =
        Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
    TopicView topicView = JSON.parseObject(jsonString, TopicView.class);

    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(StandaloneRestServer.HOST);
    Builder request = webTarget.path("topics/").request();
    Response response = request.post(Entity.json(topicView));
    Assert.assertEquals(Status.CONFLICT.getStatusCode(), response.getStatus());
  }

  @Test
  public void testCreateAndDeleteTopic() throws IOException {
    String jsonString =
        Files.toString(new File("src/test/resources/topic-sample.json"), Charsets.UTF_8);
    TopicView topicView = JSON.parseObject(jsonString, TopicView.class);
    topicView.setName(topicView.getName() + "_" + UUID.randomUUID());

    Client client = ClientBuilder.newClient();
    WebTarget webTarget = client.target(StandaloneRestServer.HOST);
    Builder request = webTarget.path("topics/").request();
    Response response = request.post(Entity.json(topicView));
    Assert.assertEquals(Status.CREATED.getStatusCode(), response.getStatus());
    TopicView createdTopic = response.readEntity(TopicView.class);
    request = webTarget.path("topics/" + createdTopic.getName()).request();
    response = request.delete();
    Assert.assertEquals(Status.OK.getStatusCode(), response.getStatus());
  }
}
