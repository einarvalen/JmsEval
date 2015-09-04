package org.mqeval;

import javax.jms.Connection;
import javax.jms.Queue;
import javax.jms.Topic;

public interface Context {
	Connection newConnection(String hostA, String hostB) throws Exception;

	Connection newConnection(String host) throws Exception;

	Topic newTopic(String topicName) throws Exception;

	Queue newQueue(String queueName) throws Exception;

	void startBroker(String hostname) throws Exception;

	void stopBroker(String hostname) throws Exception;
}
