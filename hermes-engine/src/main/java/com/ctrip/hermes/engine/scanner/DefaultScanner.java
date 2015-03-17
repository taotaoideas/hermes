package com.ctrip.hermes.engine.scanner;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.reflections.Reflections;

import com.ctrip.hermes.consumer.Consumer;
import com.ctrip.hermes.consumer.Subscribe;
import com.ctrip.hermes.engine.Subscriber;

public class DefaultScanner implements Scanner {

	@Override
	public List<Subscriber> scan() {
		List<Subscriber> subscribers = new ArrayList<Subscriber>();

		// TODO remove hard code for demo
		Reflections r = new Reflections("com.ctrip.hermes.example.demo");
		Set<Class<?>> classes = r.getTypesAnnotatedWith(Subscribe.class);

		for (Class<?> clazz : classes) {
			Subscribe anno = clazz.getAnnotation(Subscribe.class);
			Consumer<?> consumer;
			try {
				consumer = (Consumer<?>) clazz.newInstance();
				subscribers.add(new Subscriber(anno.topicPattern(), anno.groupId(), consumer));
			} catch (Exception e) {
				throw new RuntimeException(String.format("Can not create instance of class %s via reflection",
				      clazz.getName()), e);
			}
		}

		return subscribers;
	}

}
