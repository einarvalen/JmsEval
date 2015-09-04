package org.mqeval;

import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Session;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class FeatureTest {
	private final Context context = newContext();
	private JmsProducer jmsProducer = new JmsProducer(context);
	private JmsConsumer jmsConsumer = new JmsConsumer(context);
	private final static String HOST = "puppet03";

	Context newContext() {
		return new ActiveMqContext();
	}

	@Before
	public void setup() {
		jmsProducer = new JmsProducer(context);
		jmsConsumer = new JmsConsumer(context);
	}

	@Test
	public void doesJmsQueuesWork() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(17);
		Destination destination = context.newQueue("doesJmsQueuesWork");
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		List<String> incomingMessages = jmsConsumer.receive(outgoingMessages.size(), HOST, destination, DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		Assert.assertTrue(incomingMessages.equals(outgoingMessages));
	}

	@Test
	public void doesJmsTopicsWork() throws Exception {
		final List<String> outgoingMessages = MessageGenerator.generate(11);
		Destination destination = context.newTopic("doesJmsTopicsWork");
		for (int consumerCount = 0; consumerCount < 10; ++consumerCount) {
			attachConsumer(destination, outgoingMessages);
		}
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
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
	public void doesAmqpWork() throws Exception {
		final int messageCount = 10;
		AmqpProducer producer = new AmqpProducer();
		AmqpConsumer consumer = new AmqpConsumer();
		AtomicInteger receivedCount = new AtomicInteger(); 
		new Thread(() -> {
			try {
				receivedCount.set((int)consumer.receive());
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			} finally {
				Assert.assertEquals(messageCount, receivedCount.get());
			}
		}).start();
		producer.send(messageCount);
	}

	@Test
	public void doesStompWork() throws Exception {
		final int messageCount = 10;
		StompProducer producer = new StompProducer();
		StompConsumer consumer = new StompConsumer();
		AtomicInteger receivedCount = new AtomicInteger(); 
		producer.send(messageCount);
		new Thread(() -> {
			try {
				receivedCount.set((int)consumer.receive());
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			} finally {
				Assert.assertEquals(messageCount, receivedCount.get());
			}
		}).start();
	}
}
