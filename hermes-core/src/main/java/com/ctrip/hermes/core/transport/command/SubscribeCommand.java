package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SubscribeCommand extends AbstractCommand implements AckAware<SubscribeAckCommand> {

	private static final long serialVersionUID = 3599083022933864580L;

	private String m_groupId;

	private String m_topic;

	private int m_partition;

	private int m_window = 50;

	public SubscribeCommand() {
		super(CommandType.SUBSCRIBE);
	}

	public int getWindow() {
		return m_window;
	}

	public void setWindow(int window) {
		this.m_window = window;
	}

	public String getGroupId() {
		return m_groupId;
	}

	public void setGroupId(String groupId) {
		m_groupId = groupId;
	}

	public String getTopic() {
		return m_topic;
	}

	public void setTopic(String topic) {
		m_topic = topic;
	}

	public int getPartition() {
		return m_partition;
	}

	public void setPartition(int partition) {
		m_partition = partition;
	}

	@Override
	public void onAck(SubscribeAckCommand ack) {
		// TODO
	}

	@Override
	public void parse0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		m_topic = codec.readString();
		m_partition = codec.readInt();
		m_groupId = codec.readString();
		m_window = codec.readInt();
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
		codec.writeInt(m_window);
	}

	@Override
	public String toString() {
		return "SubscribeCommand [m_groupId=" + m_groupId + ", m_topic=" + m_topic + ", m_partition=" + m_partition
		      + ", m_window=" + m_window + ", m_header=" + m_header + "]";
	}

}
