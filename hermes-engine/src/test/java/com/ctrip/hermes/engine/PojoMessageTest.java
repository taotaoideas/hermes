package com.ctrip.hermes.engine;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.Test;
import org.unidal.lookup.ComponentTestCase;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Message;
import com.ctrip.hermes.engine.ConsumerBootstrap;
import com.ctrip.hermes.engine.LocalConsumerBootstrap;
import com.ctrip.hermes.engine.Subscriber;
import com.ctrip.hermes.producer.Producer;

public class PojoMessageTest extends ComponentTestCase {

	@Test
	public void test() throws Exception {
		ConsumerBootstrap b = lookup(ConsumerBootstrap.class, LocalConsumerBootstrap.ID);
		final CountDownLatch latch = new CountDownLatch(1);
		final AtomicReference<Person> got = new AtomicReference<PojoMessageTest.Person>();

		String topic = "order.new";
		Subscriber s = new Subscriber(topic, "group1", new Consumer<Person>() {

			@Override
			public void consume(List<Message<Person>> msgs) {
				for (Message<Person> m : msgs) {
					got.set(m.getBody());
					latch.countDown();
				}
			}
		}, Person.class);

		b.startConsumer(s);

		Person send = new Person("mm", 11);
		Producer.getInstance().message(topic, send).withKey("11").send();

		assertTrue(latch.await(1, TimeUnit.SECONDS));

		assertEquals(send, got.get());
		assertFalse(send == got.get());

	}

	public static class Person {
		private String name;

		private int age;

		public Person(String name, int age) {
			this.name = name;
			this.age = age;
		}

		public Person() {
		}

		public String getName() {
			return name;
		}

		public void setName(String name) {
			this.name = name;
		}

		public int getAge() {
			return age;
		}

		public void setAge(int age) {
			this.age = age;
		}

		@Override
		public String toString() {
			return "Person [name=" + name + ", age=" + age + "]";
		}

		@Override
		public int hashCode() {
			final int prime = 31;
			int result = 1;
			result = prime * result + age;
			result = prime * result + ((name == null) ? 0 : name.hashCode());
			return result;
		}

		@Override
		public boolean equals(Object obj) {
			if (this == obj)
				return true;
			if (obj == null)
				return false;
			if (getClass() != obj.getClass())
				return false;
			Person other = (Person) obj;
			if (age != other.age)
				return false;
			if (name == null) {
				if (other.name != null)
					return false;
			} else if (!name.equals(other.name))
				return false;
			return true;
		}

	}

}
