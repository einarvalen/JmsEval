package org.mqeval;

import java.util.Arrays;
import java.util.List;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.MessageProducer;
import javax.jms.Session;

public class JmsProducer extends JmsCommon {
	
	public JmsProducer(Context context) {
		super(context);
	}

	public void send(List<String> messages, String hostA, String hostB, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		send(messages, connect(hostA,hostB), dest, deliveryMode, acknowledgeMode);
	}

	public void send(List<String> messages, String host, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		send(messages, connect(host), dest, deliveryMode, acknowledgeMode);
	}
	
	public void send(String message, String host, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		send(Arrays.asList(new String[]{message}), connect(host), dest, deliveryMode, acknowledgeMode);
	}

	public void send(List<String> messages, Connection connection, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		boolean isTransacted = (acknowledgeMode == Session.SESSION_TRANSACTED);
		try {
			connection.start();
			Session session = connection.createSession(isTransacted, acknowledgeMode);
			try {
				MessageProducer producer = session.createProducer(dest);
				producer.setDeliveryMode(deliveryMode);
				for (String message : messages) {
					producer.send(session.createTextMessage(message));
				}
				if (isTransacted) session.commit();
			} catch (Exception e) {
				if (isTransacted) session.rollback();
				throw e;
			} finally {
				close(session);
			}
		} finally {
			close(connection);
		}
	}

}
