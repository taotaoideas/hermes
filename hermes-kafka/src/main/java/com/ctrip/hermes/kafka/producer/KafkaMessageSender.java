package com.ctrip.hermes.kafka.producer;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.unidal.lookup.annotation.Inject;
import org.unidal.lookup.annotation.Named;

import com.ctrip.hermes.core.env.ClientEnvironment;
import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.core.result.SendResult;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Endpoint;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.producer.sender.MessageSender;

@Named(type = MessageSender.class, value = Endpoint.KAFKA)
public class KafkaMessageSender implements MessageSender {

	private Map<String, KafkaProducer<String, byte[]>> m_producers = new HashMap<>();;

	private Map<String, MessageCodec> m_codecs = new HashMap<>();

	@Inject
	private MetaService m_metaService;

	@Inject
	private ClientEnvironment m_environment;

	private Properties getProduerProperties(String topic) {
		Properties configs = new Properties();

		try {
			Properties envProperties = m_environment.getProducerConfig(topic);
			configs.putAll(envProperties);
		} catch (IOException e) {
			e.printStackTrace();
		}

		List<Partition> partitions = m_metaService.getPartitions(topic);
		if (partitions == null || partitions.size() < 1) {
			return configs;
		}

		String producerDatasource = partitions.get(0).getWriteDatasource();
		Storage produerStorage = m_metaService.findStorage(topic);
		if (produerStorage == null) {
			return configs;
		}

		for (Datasource datasource : produerStorage.getDatasources()) {
			if (producerDatasource.equals(datasource.getId())) {
				for (Property prop : datasource.getProperties()) {
					configs.put(prop.getName(), prop.getValue());
				}
				break;
			}
		}
		configs.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		configs.put("key.serializer", StringSerializer.class.getCanonicalName());
		if (!configs.containsKey("client.id")) {
			try {
				configs.put("client.id", InetAddress.getLocalHost().getHostAddress() + "_" + topic);
			} catch (UnknownHostException e) {
				e.printStackTrace();
			}
		}
		return configs;
	}

	@Override
	public Future<SendResult> send(ProducerMessage<?> msg) {
		String topic = msg.getTopic();
		String partition = msg.getPartition();

		if (!m_producers.containsKey(topic)) {
			Properties configs = getProduerProperties(topic);
			KafkaProducer<String, byte[]> producer = new KafkaProducer<>(configs);
			m_producers.put(topic, producer);

			MessageCodec codec = new DefaultMessageCodec(topic);
			m_codecs.put(topic, codec);
		}

		KafkaProducer<String, byte[]> producer = m_producers.get(topic);
		MessageCodec codec = m_codecs.get(topic);

		ByteBuf byteBuf = Unpooled.buffer();
		codec.encode(msg, byteBuf);

		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, byteBuf.array());
		Future<RecordMetadata> sendResult = producer.send(record);
		return new KafkaFuture(sendResult);
	}

	class KafkaFuture implements Future<SendResult> {

		private Future<RecordMetadata> recordMetadata;

		public KafkaFuture(Future<RecordMetadata> recordMetadata) {
			this.recordMetadata = recordMetadata;
		}

		@Override
		public boolean cancel(boolean mayInterruptIfRunning) {
			return this.recordMetadata.cancel(mayInterruptIfRunning);
		}

		@Override
		public SendResult get() throws InterruptedException, ExecutionException {
			this.recordMetadata.get();
			SendResult sendResult = new SendResult(true);
			return sendResult;
		}

		@Override
		public SendResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
		      TimeoutException {
			this.recordMetadata.get(timeout, unit);
			SendResult sendResult = new SendResult(true);
			return sendResult;
		}

		@Override
		public boolean isCancelled() {
			return this.recordMetadata.isCancelled();
		}

		@Override
		public boolean isDone() {
			return this.recordMetadata.isDone();
		}

	}
}
