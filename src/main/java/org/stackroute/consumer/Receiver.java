package org.stackroute.consumer;

import java.util.concurrent.CountDownLatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;

//The Receiver is nothing more than a simple POJO that defines a method for receiving messages
public class Receiver {

	// logger to log all messages.
	private static final Logger LOGGER = LoggerFactory.getLogger(Receiver.class);

	// For testing convenience, we added a CountDownLatch. This allows the POJO
	// to signal that a message is received. This is something we will not
	// implement in a production application.
	private CountDownLatch latch = new CountDownLatch(1);

	public CountDownLatch getLatch() {
		return latch;
	}

	// The @KafkaListener annotation creates a message listener container for
	// each annotated method, using a
	// ConcurrentMessageListenerContainer.
	@KafkaListener(topics = "${kafka.topic.helloworld}")
	
	// The Receiver is nothing more than a simple POJO that defines a method for
	// receiving messages
	public void receive(String message) {
		LOGGER.info("received message='{}'", message);
		latch.countDown();
	}
}
