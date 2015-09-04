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
			System.out.println("send - 1");
			connection.start();
			System.out.println("send - 2");
			Session session = connection.createSession(isTransacted, acknowledgeMode);
			System.out.println("send - 3");
			try {
				MessageProducer producer = session.createProducer(dest);
				System.out.println("send - 4");
				producer.setDeliveryMode(deliveryMode);
				System.out.println("send - 5");
				for (String message : messages) {
					System.out.println("send - 6");
					producer.send(session.createTextMessage(message));
					System.out.println("send - 7");
				}
				if (isTransacted) session.commit();
				System.out.println("send - 8");
			} catch (Exception e) {
				if (isTransacted) session.rollback();
				System.out.println("send - 9");
				throw e;
			} finally {
				close(session);
				System.out.println("send - 10");
			}
		} finally {
			close(connection);
			System.out.println("send - Done");
		}
	}

}
