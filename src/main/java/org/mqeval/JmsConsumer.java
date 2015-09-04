package org.mqeval;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.function.Consumer;

import javax.jms.Connection;
import javax.jms.Destination;
import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.MessageConsumer;
import javax.jms.MessageListener;
import javax.jms.Session;
import javax.jms.TextMessage;
import javax.jms.Topic;
import javax.jms.TopicSubscriber;

public class JmsConsumer {
	private Context context;

	public JmsConsumer(Context context) {
		this.context = context;
	}

	public List<String> receive(int count, String hostA, String hostB, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		Connection connection = context.newConnection(hostA,hostB);
		return receive(count, connection, dest, deliveryMode, acknowledgeMode);
	}
	
	public List<String> receive(int count, String host, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		Connection connection = context.newConnection(host);
		return receive(count, connection, dest, deliveryMode, acknowledgeMode);
	}		

	public List<String> receive(int count, Connection connection, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		boolean isTransacted = (acknowledgeMode == Session.SESSION_TRANSACTED);
		List<String> messages = new ArrayList<>();
		try {
			connection.start();
			Session session = connection.createSession(isTransacted, acknowledgeMode);
			try {
				MessageConsumer consumer = session.createConsumer(dest);
				for (int i = 0; i < count; ++i) {
					Message msg = consumer.receive();
					if (msg instanceof TextMessage) {
						messages.add(((TextMessage) msg).getText());
					} else {
						throw new IllegalArgumentException("Unexpected message type: " + msg.getClass());
					}
				}
				if (isTransacted) session.commit();
			} catch (Exception e) {
				if (isTransacted) session.rollback();
				throw e;
			} finally {
				close(session);
			}
		} finally {
			close(connection);
		}
		return messages;
	}

	public void listen(Consumer<String> func, String hostA, String hostB, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		listen(func, context.newConnection(hostB,hostB), dest, deliveryMode, acknowledgeMode);
	}
	
	public void listen(Consumer<String> func, String host, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		listen(func, context.newConnection(host), dest, deliveryMode, acknowledgeMode);
	}
	
	public void listen(Consumer<String> func, Connection connection, Destination dest, int deliveryMode, int acknowledgeMode) throws Exception {
		boolean isTransacted = (acknowledgeMode == Session.SESSION_TRANSACTED);
		try {
			connection.start();
			Session session = connection.createSession(isTransacted, acknowledgeMode);
			try {
				MessageConsumer consumer = session.createConsumer(dest);
				Message msg = consumer.receive();
				if (msg instanceof TextMessage) {
					System.out.println("listen- func - 1");
					func.accept(((TextMessage) msg).getText());
					System.out.println("listen- func - 2");
				} else {
					throw new IllegalArgumentException("Unexpected message type: " + msg.getClass());
				}
				if (isTransacted) session.commit();
			} catch (Exception e) {
				if (isTransacted) session.rollback();
				throw e;
			} finally {
				close(session);
			}
		} finally {
			close(connection);
			System.out.println("lisyten- func - Done");
		}
	}

	public List<String> receiveDurableSubscription(int count, String host, Topic dest, int acknowledgeMode) throws Exception {
		List<String> messages = new ArrayList<>();
		Connection connection = context.newConnection(host);
		final CountDownLatch countDownLatch = new CountDownLatch(count);
		String clientID = UUID.randomUUID().toString();
		connection.setClientID(clientID);
		try {
			connection.start();
			Session session = connection.createSession(false, acknowledgeMode);
			try {
				TopicSubscriber subscriber = session.createDurableSubscriber(dest, clientID);
				subscriber.setMessageListener(new MessageListener() {
					@Override
					public void onMessage(Message msg) {
						if (msg instanceof TextMessage) {
							try {
								messages.add(((TextMessage) msg).getText());
								countDownLatch.countDown();
							} catch (JMSException e) {
								throw new RuntimeException("MessageListener.onMessage() failed", e);
							}
						} else {
							throw new IllegalArgumentException("Unexpected message type: " + msg.getClass());
						}
					}
				});
				countDownLatch.await();
			} finally {
				close(session);
			}
		} finally {
			close(connection);
		}
		return messages;
	}

	private void close(Connection resource) {
		try {
			if (resource != null) resource.close();
		} catch (Exception e) {}
	}
	
	private void close(Session resource) {
		try {
			if (resource != null) resource.close();
		} catch (Exception e) {}
	}
	
}
