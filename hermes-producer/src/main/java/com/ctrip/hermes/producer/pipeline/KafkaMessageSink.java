package com.ctrip.hermes.producer.pipeline;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
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

import com.ctrip.hermes.message.StoredMessage;
import com.ctrip.hermes.meta.MetaService;
import com.ctrip.hermes.meta.entity.Property;
import com.ctrip.hermes.meta.entity.Storage;
import com.ctrip.hermes.pipeline.PipelineContext;
import com.ctrip.hermes.pipeline.PipelineSink;
import com.ctrip.hermes.producer.ProducerMessage;
import com.ctrip.hermes.producer.api.SendResult;
import com.ctrip.hermes.producer.codec.ProducerMessageCodec;

public class KafkaMessageSink implements PipelineSink<Future<SendResult>> {

	public static final String ID = "kafka";

	private Map<String, KafkaProducer<String, byte[]>> m_producers;

	@Inject
	private StoredMessageCodec m_storedCodec;

	@Inject
	private ProducerMessageCodec m_codec;
	
	@Inject
	private MetaService m_metaService;

	public KafkaMessageSink() {
		m_producers = new HashMap<>();
	}

	private Properties getProduerProperties(String topic) {
		Properties configs = new Properties();
		String endpointType = m_metaService.getEndpointType(topic);
		Storage targetStorage = null;
		for (Storage storage : connector.getStorages()) {
			if ("producer".equalsIgnoreCase(storage.getType())) {
				targetStorage = storage;
				break;
			}
		}
		for (Property prop : targetStorage.getProperties()) {
			configs.put(prop.getName(), prop.getValue());
		}
		configs.put("value.serializer", ByteArraySerializer.class.getCanonicalName());
		configs.put("acks", "1");
		configs.put("key.serializer", StringSerializer.class.getCanonicalName());
		return configs;
	}

	@Override
	public Future<SendResult> handle(PipelineContext<Future<SendResult>> ctx, Object input) {
		String topic = ctx.get("topic");
		String partition = ctx.get("partition");

		if (!m_producers.containsKey(topic)) {
			Properties configs = getProduerProperties(topic);
			KafkaProducer<String, byte[]> producer = new KafkaProducer<>(configs);
			m_producers.put(topic, producer);
		}

		KafkaProducer<String, byte[]> producer = m_producers.get(topic);

		// raw msg
		ByteBuffer rawMsgBuf = (ByteBuffer) input;
		rawMsgBuf.flip();
		ProducerMessage<byte[]> rawMsg = m_codec.decode(rawMsgBuf);
		// stored msg list
		ByteBuffer storedMsg = m_storedCodec.encode(Arrays.asList(new StoredMessage<byte[]>(rawMsg)));
		storedMsg.flip();
		byte[] storedMsgBytes = new byte[storedMsg.remaining()];
		storedMsg.get(storedMsgBytes, 0, storedMsgBytes.length);

		ProducerRecord<String, byte[]> record = new ProducerRecord<>(topic, partition, storedMsgBytes);
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
			RecordMetadata record = this.recordMetadata.get();
			// TODO Fill sendResult
			SendResult sendResult = new SendResult();
			return sendResult;
		}

		@Override
		public SendResult get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException,
		      TimeoutException {
			RecordMetadata record = this.recordMetadata.get(timeout, unit);
			// TODO Fill sendResult;
			SendResult sendResult = new SendResult();
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
