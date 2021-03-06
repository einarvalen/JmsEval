package org.mqeval;

import javax.jms.Connection;
import javax.jms.JMSException;
import javax.jms.Queue;
import javax.jms.Topic;

import org.apache.activemq.ActiveMQConnectionFactory;
import org.apache.activemq.command.ActiveMQQueue;
import org.apache.activemq.command.ActiveMQTopic;

public class ActiveMqContext implements Context {
	private static String user = "system";
	private static String password = "manager";
	private static int port = 61616;

	@Override
	public Connection newConnection(String host) {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory("tcp://" + host + ":" + port);
		try {
			return factory.createConnection(user, password);
		} catch (JMSException e) {
			throw new RuntimeException("newConnection() failed", e);
		}
	}

	@Override
	public Connection newConnection(String hostA, String hostB) {
		ActiveMQConnectionFactory factory = new ActiveMQConnectionFactory(String.format("failover:(tcp://%s:%d,tcp://%s:%d)",hostA,port,hostB,port));
		try {
			return factory.createConnection(user, password);
		} catch (JMSException e) {
			throw new RuntimeException("newConnection() failed", e);
		}
	}
	@Override
	public Topic newTopic(String topicName) throws Exception {
		return new ActiveMQTopic(topicName);
	}

	@Override
	public Queue newQueue(String queueName) throws Exception {
		return new ActiveMQQueue(queueName);
	}
	
	@Override
	public void stopBroker(String hostname) throws Exception {
		activeMqCommand(hostname, "stop");
	}

	@Override
	public void startBroker(String hostname) throws Exception {
		activeMqCommand(hostname, "start");
	}
	
	private void activeMqCommand(String hostname, String command) throws Exception {
		final Runtime rt = Runtime.getRuntime();
		Process proc = rt.exec(String.format("activemq.sh %s %s", hostname, command));
		proc.waitFor();
	}

}
