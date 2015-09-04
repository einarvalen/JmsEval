package org.mqeval;

import javax.jms.DeliveryMode;

import org.apache.qpid.amqp_1_0.jms.Connection;
import org.apache.qpid.amqp_1_0.jms.MessageProducer;
import org.apache.qpid.amqp_1_0.jms.Session;
import org.apache.qpid.amqp_1_0.jms.TextMessage;
import org.apache.qpid.amqp_1_0.jms.impl.ConnectionFactoryImpl;
import org.apache.qpid.amqp_1_0.jms.impl.TopicImpl;

public class AmqpProducer {
	String user = "system";
	String password = "manager";
	String host = "puppet03";
	int port = 5672;
	String destination = "topic://amqp-event";

	public void send( int messageCount) throws Exception {
		ConnectionFactoryImpl factory = new ConnectionFactoryImpl(host, port, user, password);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(new TopicImpl(destination));
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		for (String message : MessageGenerator.generate(messageCount)) {
			TextMessage msg = session.createTextMessage(message);
			producer.send(msg);
		}
		producer.send(session.createTextMessage("SHUTDOWN"));
		connection.close();
	}
}
