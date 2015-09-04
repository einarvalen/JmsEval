package org.mqeval;

import java.util.*;

public class MessageGenerator {
	
	public static List<String> generate(int count) {
		List<String> messages = new ArrayList<>();
		for (int i = 0; i < count; ++i) {
			messages.add("Message-Body-#" + i);
		}
		return messages;
	}

}
