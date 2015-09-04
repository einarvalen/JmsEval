package org.mqeval;


import org.apache.qpid.amqp_1_0.jms.Connection;
import org.apache.qpid.amqp_1_0.jms.Message;
import org.apache.qpid.amqp_1_0.jms.MessageConsumer;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;

public class AmqpConsumer {
	String user = "system";
	String password = "manager";
	String host = "puppet03";
	int port = 5672;
	String destination = "topic://amqp-event";

	public long receive() throws Exception {
		ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port, user, password);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageConsumer consumer = session.createConsumer(new TopicImpl(destination));
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
				throw new IllegalArgumentException("AmqpConsumer.receive() - Unexpected message type: " + msg.getClass());
			}
		}

	}
}
