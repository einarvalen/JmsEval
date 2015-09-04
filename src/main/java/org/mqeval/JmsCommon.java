package org.mqeval;

import java.util.function.Supplier;

import javax.jms.Connection;
import javax.jms.Session;

public class JmsCommon {
	private Context context;
	
	public JmsCommon(Context context) {
		this.context = context;
	}

	public Connection connect(String hostA, String hostB) throws Exception {
		return connect(() -> context.newConnection(hostA, hostB));
	}
	
	public Connection connect(String host) throws Exception {
		return connect(() -> context.newConnection(host));
	}

	private Connection connect(Supplier<Connection> sup) throws Exception {
		long waitTime = 5*1000;
		Exception lastException = new Exception("JmsConnect failed");
		for (int retries = 0; retries < 3; ++retries) {
			try {
				return sup.get();
			} catch (Exception e) {
				lastException = e;
				try {
					Thread.sleep(waitTime);
				} catch (InterruptedException ei) {
					Thread.currentThread().interrupt();
				}
			}
		}
		throw lastException;
	}

	public void close(Connection resource) {
		try {
			if (resource != null) resource.close();
		} catch (Exception e) {}
	}
	
	public void close(Session resource) {
		try {
			if (resource != null) resource.close();
		} catch (Exception e) {}
	}
}
