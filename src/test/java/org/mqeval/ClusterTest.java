package org.mqeval;

import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.jms.DeliveryMode;
import javax.jms.Destination;
import javax.jms.Session;

import junit.framework.Assert;

import org.junit.Before;
import org.junit.Test;

public class ClusterTest {

	private final Context context = newContext();
	private JmsProducer jmsProducer = null;
	private JmsConsumer jmsConsumer = null;
	private final static String HostA = "puppet01", HostB = "puppet02";

	Context newContext() {
		return new ActiveMqContext();
	}

	@FunctionalInterface
	private interface MyTask {
		void run() throws Exception;
	}
	
	private void execInThread( MyTask task) {
		new Thread(() -> {
			try {
				task.run();
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			}
		}).start();
	}
	
	@Before
	public void setup() {
		jmsProducer = new JmsProducer(context);
		jmsConsumer = new JmsConsumer(context);
	}

	/*
	Clustering-1. - Will consumers and producers fail-over to another broker when the current connections fail?
		- Connect a producer and a consumer to a clustered broker A.
		- Kill broker A.
		- Verify that producer and consumer connections are taken over by another broker.
		- Restart broker A.
	*/
	@Test
	public void areConnectionsTransferedAfterBrokerCrash() throws Exception {
		final int messageCount = 11;
		final AtomicBoolean allMessagesWasSent = new AtomicBoolean(false);
		final CountDownLatch allMessagesWasReceived = new CountDownLatch(1);
		Destination destination = context.newQueue("areConnectionsTransferedAfterBrokerCrashQueue");
		execInThread(() -> {
			jmsConsumer.receive(messageCount, HostA, HostB, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
			allMessagesWasReceived.countDown();
		});
		context.stopBroker(HostA);
		Thread.sleep(15*1000);
		execInThread(() -> {
			jmsProducer.send(MessageGenerator.generate(messageCount), HostA, HostB, destination, DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
			allMessagesWasSent.set(true);
		});
		allMessagesWasReceived.await(15, TimeUnit.SECONDS);
		context.startBroker(HostA);
		Assert.assertTrue(allMessagesWasSent.get());
	}
	

	/*
	Clustering-2 - When a broker fails and another broker takes over, can persistent messages be lost?
		- Connect a producer and a consumer to a persistent queue in broker A and start transmitting messages.
		- Kill broker A.
		- Verify that the clients are reconnected to another broker, B, and message transmitting resumed.
		- Restart broker A.
		- Verify the all messages sent are received by the consumer - and only once.
	*/
	@Test
	public void willAllPersistentMessagesArriveAndOnlyOnceAfterBrokerCrash() throws Exception {
		willAllMessagesArriveAndOnlyOnceAfterBrokerCrash(DeliveryMode.PERSISTENT);
	}
	
	
	/*
	Clustering-3 - When a broker fails and another broker takes over, can non-persistent messages be lost?
		- Run test scenario as in Clusering-1, but connect to a non-persistent queue.
		- Restart broker A.
		- Verify the all messages sent are received by the consumer - and only once.
	*/
	@Test
	public void willAllNonPersistentMessagesArriveAndOnlyOnceAfterBrokerCrash() throws Exception {
		willAllMessagesArriveAndOnlyOnceAfterBrokerCrash(DeliveryMode.NON_PERSISTENT);
	}
	
	private void willAllMessagesArriveAndOnlyOnceAfterBrokerCrash(int deliveryMode) throws Exception {
		final int messageCount = 20;
		final CountDownLatch allMessagesWasReceived = new CountDownLatch(1);
		List<String> outgoingMessages = MessageGenerator.generate(messageCount);
		Destination destination = context.newQueue("willAllPersistentMessagesArriveAndOnlyOnceAfterBrokerCrashQueue");
		execInThread(() -> {
			List<String> incommingMessages = jmsConsumer.receive(messageCount, HostA, HostB, destination, deliveryMode, Session.AUTO_ACKNOWLEDGE);
			Assert.assertEquals(outgoingMessages, incommingMessages);
			allMessagesWasReceived.countDown();
		});
		execInThread(() -> {
			jmsProducer.send(outgoingMessages.subList(0, messageCount / 2), HostA, HostB, destination, deliveryMode, Session.AUTO_ACKNOWLEDGE);
			context.stopBroker(HostA);
			Thread.sleep(5*1000);
			jmsProducer.send(outgoingMessages.subList(messageCount / 2, messageCount), HostA, HostB, destination, deliveryMode, Session.AUTO_ACKNOWLEDGE);
		});
		allMessagesWasReceived.await(15, TimeUnit.SECONDS);
		context.startBroker(HostA);
	}

	/*
	Clustering-4 - Are connections evenly distributed across the brokers in a cluster?
		- Create a set queues with producers and consumers.
		- Observe how this distributes across available brokers.
		- Expect the number of producers and consumers to be approximately evenly spread out.
	 */
	@Test
	public void areConnectionsEvenlyDistributed() throws Exception {
		final int queueCount = 30;
		final CountDownLatch countDownLatch = new CountDownLatch(1);
		for (int i = 0; i < queueCount; ++i) {
			Destination destination = context.newQueue("areConnectionsEvenlyDistributed" + i);	
			execInThread(() -> jmsConsumer.listenForOneMessage((message) -> await(countDownLatch), HostA, HostB, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE));
			execInThread(() -> jmsProducer.send(MessageGenerator.generate(1), HostA, HostB, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE));
		}
		Thread.sleep(30*1000);
		countDownLatch.countDown();
	}
	
	private void await(CountDownLatch countDownLatch) {
		try {
			countDownLatch.await(30,TimeUnit.SECONDS);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

}
