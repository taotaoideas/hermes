package com.ctrip.hermes.core.transport.command;

import io.netty.buffer.ByteBuf;

import com.ctrip.hermes.core.utils.HermesPrimitiveCodec;

/**
 * @author Leo Liang(jhliang@ctrip.com)
 *
 */
public class SubscribeCommand extends AbstractCommand implements AckAware<SubscribeAckCommand> {

	private String m_groupId;

	private String m_topic;

	private int m_partition;

	public SubscribeCommand() {
		super(CommandType.SUBSCRIBE);
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
	}

	@Override
	public void toBytes0(ByteBuf buf) {
		HermesPrimitiveCodec codec = new HermesPrimitiveCodec(buf);

		codec.writeString(m_topic);
		codec.writeInt(m_partition);
		codec.writeString(m_groupId);
	}

}
