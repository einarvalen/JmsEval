package org.mqeval;

import java.util.List;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Session;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class PerformanceTest {
	private final Context context = newContext();
	private JmsProducer jmsProducer = null;
	private JmsConsumer jmsConsumer = null;
	private final static String HOST = "puppet03";

	Context newContext() {
		return new ActiveMqContext();
	}

	@Before
	public void setup() {
		jmsProducer = new JmsProducer(context);
		jmsConsumer = new JmsConsumer(context);
	}

	private void attachConsumer(Destination destination, final List<String> outgoingMessages) {
		new Thread(() -> {
			int receivedCount = 0;
			try {
				List<String> incomingMessages = jmsConsumer.receive(outgoingMessages.size(), HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
				receivedCount = incomingMessages.size();
				Assert.assertTrue(incomingMessages.equals(outgoingMessages));
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			} finally {
				Assert.assertEquals(outgoingMessages.size(), receivedCount);
			}
		}).start();
	}
	
	@Test
	public void findMaxNumberOfQueues() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(1);
		int max = 2;
		for (int queueCount = 0; queueCount < max; ++queueCount) {
			jmsProducer.send(outgoingMessages, HOST, context.newQueue("countQueues-" + queueCount), DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		}
		for (int queueCount = 0; queueCount < max; ++queueCount) {
			List<String> incomingMessages = jmsConsumer.receive(outgoingMessages.size(), HOST, context.newQueue("countQueues-" + queueCount), DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
			Assert.assertTrue(incomingMessages.equals(outgoingMessages));
		}
	}

	@Test
	public void findMaxNumberOfTopics() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(1);
		int max = 2;
		for (int topicCount = 0; topicCount < max; ++topicCount) {
			attachConsumer(context.newTopic("countTopics-" + topicCount), outgoingMessages);
		}
		for (int topicCount = 0; topicCount < max; ++topicCount) {
			jmsProducer.send(outgoingMessages, HOST, context.newTopic("countTopics-" + topicCount), DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		}
	}

	@Test
	public void findMaxNumberOfTopicConsumers() throws Exception {
		int max = 2;
		List<String> outgoingMessages = MessageGenerator.generate(max);
		Destination destination = context.newTopic("countTopicConsumers");
		for (int consumerCount = 0; consumerCount < max; ++consumerCount) {
			attachConsumer(destination, outgoingMessages);
		}
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
	}

	@Test
	public void findMaxNumberOfQueueConsumers() throws Exception {
		int max = 2;
		List<String> outgoingMessages = MessageGenerator.generate(max);
		Destination destination = context.newQueue("countQueueConsumers");
		for (int consumerCount = 0; consumerCount < max; ++consumerCount) {
			attachConsumer(destination, outgoingMessages);
		}
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
	}

	@Test
	public void findMaxNumberOfQueueProducers() throws Exception {
		int max = 2;
		List<String> outgoingMessages = MessageGenerator.generate(max);
		Destination destination = context.newQueue("countQueueProducers");
		attachConsumer(destination, outgoingMessages);
		for (int producerCount = 0; producerCount < max; ++producerCount) {
			jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		}
	}

}
