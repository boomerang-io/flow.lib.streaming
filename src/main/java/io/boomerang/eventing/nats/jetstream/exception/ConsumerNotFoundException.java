package io.boomerang.eventing.nats.jetstream.exception;

public class ConsumerNotFoundException extends RuntimeException {

  public ConsumerNotFoundException(String message) {
    super(message);
  }
}
