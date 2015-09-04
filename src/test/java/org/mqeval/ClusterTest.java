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

	private void attachProducer(Destination destination, List<String> outgoingMessages, AtomicBoolean allMessagesWasSent) {
		new Thread(() -> {
			try {
				//jmsProducer.send(outgoingMessages, HostA, HostB, destination, DeliveryMode.PERSISTENT, Session.AUTO_ACKNOWLEDGE);
				System.out.println("attachProducer-1");
				jmsProducer.send(outgoingMessages, HostA, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
				System.out.println("attachProducer-2");
				allMessagesWasSent.set(true);
				System.out.println("attachProducer-Done");
			} catch (Exception e) {
				Assert.fail(e.getMessage());
			}
		}).start();
	}

	private void attachConsumer(Destination destination, final int messageCount, final CountDownLatch countDownLatch) {
		new Thread(() -> {
			System.out.println("attachConsumer-1");
			try {
				jmsConsumer.receive(messageCount, HostA, destination, DeliveryMode.NON_PERSISTENT, Session.AUTO_ACKNOWLEDGE);
				countDownLatch.countDown();
				System.out.println("attachConsumer-Done");
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
		final int messageCount = 12;
		final AtomicBoolean allMessagesWasSent = new AtomicBoolean(false);
		final CountDownLatch allMessagesWasReceived = new CountDownLatch(1);
		Destination destination = context.newQueue("areConnectionsTransferedAfterBrokerCrashQueue");
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 1");
		attachConsumer(destination, messageCount, allMessagesWasReceived);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 2");
		context.stopBroker(HostA);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 3");
		Thread.sleep(15*1000);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 4");
		attachProducer(destination, MessageGenerator.generate(messageCount), allMessagesWasSent);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 5");
		allMessagesWasReceived.await(15, TimeUnit.SECONDS);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - 6");
		Assert.assertTrue(allMessagesWasSent.get());
		context.startBroker(HostA);
		System.out.println("areConnectionsTransferedAfterBrokerCrash - Done");
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
		
	}
	
	/*
	Clustering-3 - When a broker fails and another broker takes over, can non-persistent messages be lost?
		- Run test scenario as in Clusering-1, but connect to a non-persistent queue.
		- Restart broker A.
		- Verify the all messages sent are received by the consumer - and only once.
	*/
	@Test
	public void willAllNonPersistentMessagesArriveAndOnlyOnceAfterBrokerCrash() throws Exception {
		
	}
	
	/*
	Clustering-4 - Are connections evenly distributed across the brokers in a cluster?
		- Create one set of producers and another set of consumers to a queue.
		- Observe how the connection distribute across available brokers.
		- Expect the number of producers and consumers to be evenly spread out.
	 */
	@Test
	public void areConnectionsEvenlyDistributed() throws Exception {
		
	}

}
