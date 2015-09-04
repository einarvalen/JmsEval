package org.mqeval;

import javax.jms.Connection;
import javax.jms.DeliveryMode;
import javax.jms.MessageProducer;
import javax.jms.Session;

import org.fusesource.stomp.jms.StompJmsConnectionFactory;
import org.fusesource.stomp.jms.StompJmsDestination;

public class StompProducer {
	String user = "system";
	String password = "manager";
	String host = "puppet03";
	int port = 61613;
	String destination = "/topic/stomp-event";

	public void send( int messageCount) throws Exception {
		StompJmsConnectionFactory factory = new StompJmsConnectionFactory();
		factory.setBrokerURI("tcp://" + host + ":" + port);
		Connection connection = factory.createConnection(user, password);
		connection.start();
		Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
		MessageProducer producer = session.createProducer(new StompJmsDestination(destination));
		producer.setDeliveryMode(DeliveryMode.NON_PERSISTENT);
		for (String message : MessageGenerator.generate(messageCount)) {
			producer.send(session.createTextMessage(message));
		}
		producer.send(session.createTextMessage("SHUTDOWN"));
		connection.close();
	}
}
