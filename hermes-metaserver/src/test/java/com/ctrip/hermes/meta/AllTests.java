package com.ctrip.hermes.meta;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

import com.ctrip.hermes.meta.rest.CodecServerTest;
import com.ctrip.hermes.meta.rest.MetaServerTest;
import com.ctrip.hermes.meta.rest.SchemaServerTest;
import com.ctrip.hermes.meta.rest.TopicServerTest;

@RunWith(Suite.class)
@SuiteClasses({ CodecServerTest.class, MetaServerTest.class, SchemaServerTest.class, TopicServerTest.class })
public class AllTests {

}
