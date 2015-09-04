package org.mqeval;

import javax.jms.Connection;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.Session;
import javax.jms.TextMessage;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

public class StompConsumer {

	public long receive() throws Exception {
		String user = "system";
		String password = "manager";
		String host = "puppet03";
		int port = 61613;
		String destination = "/topic/stomp-event";
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = session.createConsumer(new StompJmsDestination(destination));
		long count = 0;
		while (true) {
			Message msg = consumer.receive();
			if (msg instanceof TextMessage) {
				String body = ((TextMessage) msg).getText();
				if ("SHUTDOWN".equals(body)) {
					connection.close();
					return count;
				} else {
					count++;
				}
			} else {
				throw new IllegalArgumentException("StompConsumer.receive() - Unexpected message type: " + msg.getClass());
			}
		}
	}
}