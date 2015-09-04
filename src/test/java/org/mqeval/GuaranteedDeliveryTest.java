package org.mqeval;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Session;
import javax.jms.Topic;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;

public class GuaranteedDeliveryTest {
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

	@Test
	// Guaranteed delivery -1 - Can a transaction be resumed when a consumer transaction fails?
	public void resumeFailedConsumerTransaction() throws Exception {
		String outgoingMessage = "VeryImportantMessage";
		Destination destination = context.newQueue("VeryImportantQueue");
		jmsProducer.send(outgoingMessage, HOST, destination, DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
		AtomicBoolean consumerTransactionFailed = new AtomicBoolean(false);
		AtomicBoolean consumerTransactionResumed = new AtomicBoolean(false);
		try {
			jmsConsumer.listenForOneMessage((message) -> {
				consumerTransactionFailed.set(true);
				throw new RuntimeException("Transaction fail");
			}, HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		} catch (Exception e) {
			int gracePeriod = 200;
			Thread.sleep(gracePeriod);
			// Resume consumer transaction
			jmsConsumer.listenForOneMessage((message) -> {
				consumerTransactionResumed.set(true);
				Assert.assertEquals(outgoingMessage, message);
			}, HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		}
		Assert.assertTrue(consumerTransactionFailed.get());
		Assert.assertTrue(consumerTransactionResumed.get());
	}

	@Test
	// Guaranteed delivery-2 - Will a messaged be lost if a producer temporarily fails to deliver a message to a topic or a queue?
	public void resurectLostConnection() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(1);
		Destination destination = context.newQueue("resurectLostConnectionQueue");
		context.stopBroker(HOST);
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		context.startBroker(HOST);
		Thread.sleep(30*1000);
		List<String> incomingMessages = jmsConsumer.receive(outgoingMessages.size(), HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		Assert.assertTrue(incomingMessages.equals(outgoingMessages));
	}

	@Test
	// Guaranteed delivery-3 - If a broker fails and is restarted, are persistent messages lost?
	public void resumeAbendedBroker() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(17);
		Destination destination = context.newQueue("resumeAbendedBrokerQueue");
		jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		context.stopBroker(HOST);
		context.startBroker(HOST);
		Thread.sleep(10*1000);
		List<String> incomingMessages = jmsConsumer.receive(outgoingMessages.size(), HOST, destination, DeliveryMode.PERSISTENT, Session.SESSION_TRANSACTED);
		Assert.assertTrue(incomingMessages.equals(outgoingMessages));
	}

	@Ignore // Does not work
	@Test
	// Guaranteed delivery-4 - Can a message be delivered to a topic subscriber even if not connected when the message is published?
	public void durableSubscriber() throws Exception {
		List<String> outgoingMessages = MessageGenerator.generate(17);
		Topic destination = context.newTopic("durableSubscriberTopic");
		new Thread(() -> {
			try {
				jmsProducer.send(outgoingMessages, HOST, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			}
		}).start();
		Thread.sleep(3*1000);
		List<String> incomingMessages = jmsConsumer.receiveDurableSubscription(outgoingMessages.size(), HOST, destination, Session.AUTO_ACKNOWLEDGE);
		Assert.assertTrue(incomingMessages.equals(outgoingMessages));
	}
}
