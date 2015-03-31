package com.ctrip.hermes.producer.sender;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import javax.inject.Inject;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import com.ctrip.hermes.core.message.ProducerMessage;
import com.ctrip.hermes.core.message.codec.DefaultMessageCodec;
import com.ctrip.hermes.core.message.codec.MessageCodec;
import com.ctrip.hermes.core.meta.MetaService;
import com.ctrip.hermes.meta.entity.Datasource;
import com.ctrip.hermes.meta.entity.Partition;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.producer.api.SendResult;

public class KafkaMessageSender implements MessageSender {

	private Map<String, KafkaProducer<String, byte[]>> m_producers = new HashMap<>();;

	@Inject
	private MetaService m_metaService;

	private Properties getProduerProperties(String topic) {
		Properties configs = new Properties();
		List<Partition> partitions = m_metaService.getPartitions(topic);
		String ds = partitions.get(0).getWriteDatasource();
		Storage targetStorage = m_metaService.findStorage(topic);
		for (Datasource datasource : targetStorage.getDatasources()) {
			if (ds.equals(datasource.getId())) {
				for (Property prop : datasource.getProperties()) {
					configs.put(prop.getName(), prop.getValue());
				}
				break;
			}
		}
		configs.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		configs.put("key.serializer", StringSerializer.class.getCanonicalName());
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
		}

		KafkaProducer<String, byte[]> producer = m_producers.get(topic);

		MessageCodec m_codec = new DefaultMessageCodec(topic);
		ByteBuf byteBuf = Unpooled.buffer();
		m_codec.encode(msg, byteBuf);

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
