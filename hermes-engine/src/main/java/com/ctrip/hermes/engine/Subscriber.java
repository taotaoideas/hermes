package com.ctrip.hermes.engine;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;

import com.ctrip.hermes.consumer.Consumer;

@SuppressWarnings("rawtypes")
public class Subscriber {

	private String m_groupId;

	private String m_topicPattern;

	private Consumer m_consumer;

	public Subscriber(String topicPattern, String groupId, Consumer consumer) {
		m_topicPattern = topicPattern;
		m_groupId = groupId;
		m_consumer = consumer;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public String getTopicPattern() {
		return m_topicPattern;
	}

	public Consumer getConsumer() {
		return m_consumer;
	}

	public Class<?> getMessageClass() {
		Type genericSuperClass = m_consumer.getClass().getGenericSuperclass();
		if (genericSuperClass instanceof ParameterizedType) {
			ParameterizedType paraType = (ParameterizedType) genericSuperClass;
			Type[] actualTypeArguments = paraType.getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		} else {
			Type[] genericInterfaces = m_consumer.getClass().getGenericInterfaces();
			Type[] actualTypeArguments = ((ParameterizedType) genericInterfaces[0]).getActualTypeArguments();
			Class type = (Class) actualTypeArguments[0];
			return type;
		}
	}
}
