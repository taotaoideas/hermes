package com.ctrip.hermes.broker.remoting;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.codehaus.plexus.personality.plexus.lifecycle.phase.Initializable;
import org.codehaus.plexus.personality.plexus.lifecycle.phase.InitializationException;
import org.unidal.lookup.annotation.Inject;

import com.ctrip.hermes.broker.channel.MessageQueueManager;
import com.ctrip.hermes.core.message.DecodedProducerMessage;
import com.ctrip.hermes.core.transport.command.CommandType;
import com.ctrip.hermes.core.transport.command.SendMessageAckCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.MessageRawDataBatch;
import com.ctrip.hermes.core.transport.command.SendMessageCommand.Tpp;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessor;
import com.ctrip.hermes.core.transport.command.processor.CommandProcessorContext;
import com.mysql.jdbc.Driver;

public class SendMessageRequestProcessor implements CommandProcessor, Initializable {

	public static final String ID = "send-message-request";

	@Inject
	private MessageQueueManager m_queueManager;

	private Connection m_conn;

	@Override
	public List<CommandType> commandTypes() {
		return Arrays.asList(CommandType.MESSAGE_SEND);
	}

	@Override
	public void process(CommandProcessorContext ctx) {
		SendMessageCommand req = (SendMessageCommand) ctx.getCommand();

		Map<Tpp, MessageRawDataBatch> rawBatches = req.getMessageRawDataBatches();

		for (Map.Entry<Tpp, MessageRawDataBatch> entry : rawBatches.entrySet()) {
			Tpp tpp = entry.getKey();
			try {
				saveToMysql(entry.getValue().getMessages(), tpp);
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

		SendMessageAckCommand ack = new SendMessageAckCommand();
		ack.correlate(req);

		ctx.write(ack);

	}

	private void saveToMysql(List<DecodedProducerMessage> messages, Tpp tpp) throws SQLException {
		String sql = "insert into fuck " //
		      + "values (?,?,?,?,?,?,?)";
		PreparedStatement stmt = m_conn.prepareStatement(sql);

		for (DecodedProducerMessage msg : messages) {
			stmt.setLong(1, 1);
			stmt.setLong(2, 0);
			stmt.setTimestamp(3, new Timestamp(msg.getBornTime()));
			stmt.setString(4, msg.getKey());
			stmt.setBlob(5, new ByteArrayInputStream(msg.readAppProperties()));
			stmt.setBlob(6, new ByteArrayInputStream(msg.readSysProperties()));
			stmt.setBlob(7, new ByteArrayInputStream(msg.readBody()));

			stmt.addBatch();
		}

		stmt.executeUpdate();
	}

	@Override
	public void initialize() throws InitializationException {
		try {
			Driver.class.newInstance();
			m_conn = DriverManager.getConnection("jdbc:mysql://127.0.0.1/hermes", "root", null);
		} catch (Exception e) {
			e.printStackTrace();
			throw new InitializationException("", e);
		}
	}

}
