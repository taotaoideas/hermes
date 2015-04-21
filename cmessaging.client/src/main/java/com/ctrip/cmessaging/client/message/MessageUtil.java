package com.ctrip.cmessaging.client.message;

import java.net.InetAddress;
import java.net.UnknownHostException;

import org.apache.thrift.TException;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TJSONProtocol;

import com.ctrip.cmessaging.client.constant.AdapterConstant;
import com.ctrip.cmessaging.client.impl.MessageHeader;
import com.ctrip.hermes.core.message.ConsumerMessage;

public class MessageUtil {
	public static String getExchangeName(ConsumerMessage<byte[]> msg) {
		return msg.getProperty(AdapterConstant.CMESSAGING_EXCHANGENAME);
	}

	public static String getMessageId(ConsumerMessage<byte[]> msg) {
		return msg.getProperty(AdapterConstant.CMESSAGING_MESSAGEID);
	}

	public static String getHeader(ConsumerMessage<byte[]> msg) {
		return msg.getProperty(AdapterConstant.CMESSAGING_HEADER);
	}

	public static String buildHeader(String exchangeName, String messageId, String topic, String msg,
												MessageHeader map) {
		String encodedHeader;

		try {
			BasicHeader header = new BasicHeader();
			header.setMessageID(messageId);
			header.setExchangeName(exchangeName);
			header.setContentLength(msg.getBytes().length);
			header.setContentEncoding("uft-8");
			header.setVersion("cmessage-adapter 1.0");
			header.setSerialization(com.ctrip.cmessaging.client.content.SerializationType.Json);
			header.setType(com.ctrip.cmessaging.client.content.MessageType.Text);
			header.setCompression(com.ctrip.cmessaging.client.content.CompressionType.None);

			header.setSubject(topic);
			header.setCorrelationID(map.getCorrelationID());
			header.setSequence(map.getSequence());
			header.setClientID(InetAddress.getLocalHost().getHostName());
			header.setAppID(map.getAppID());
			header.setRoute(InetAddress.getLocalHost().getHostAddress());
			header.setRawType("java.lang.String");
			header.setTimestamp(System.currentTimeMillis());
			header.setUserHeader(map.getUserHeader());

			// serialize
			TSerializer jsonSerializer = new TSerializer(new TJSONProtocol.Factory());
			encodedHeader = jsonSerializer.toString(header);

		} catch (TException e) {
			throw new RuntimeException(e);
		} catch (UnknownHostException e) {
			throw new RuntimeException(e);
		}
		return encodedHeader;
	}
}
