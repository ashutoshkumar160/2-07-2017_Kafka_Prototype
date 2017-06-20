package org.stackroute.producer;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;
import org.springframework.util.concurrent.ListenableFutureCallback;

public class Sender {

	// logger to log all messages.
	private static final Logger LOGGER = LoggerFactory.getLogger(Sender.class);

	// using the KafkaTemplate here which wraps a Producer and provides
	// convenience methods to send data to Kafka topics.
	@Autowired
	private KafkaTemplate<String, String> kafkaTemplate;

	public void send(String topic, String message) {
		// the KafkaTemplate provides asynchronous send methods returning a
		// Future
		ListenableFuture<SendResult<String, String>> future = kafkaTemplate.send(topic, message);

		// register a callback with the listener to receive the result of the
		// send asynchronously
		future.addCallback(new ListenableFutureCallback<SendResult<String, String>>() {

			// simple success method.
			@Override
			public void onSuccess(SendResult<String, String> result) {
				LOGGER.info("sent message='{}' with offset={}", message, result.getRecordMetadata().offset());
			}

			// simple failure method.
			@Override
			public void onFailure(Throwable ex) {
				LOGGER.error("unable to send message='{}'", message, ex);
			}
		});
		// or, to block the sending thread to await the result, invoke the
		// future's get() method
	}

}
