package io.boomerang.eventing.nats.jetstream.exception;

public class StreamNotFoundException extends RuntimeException {

  public StreamNotFoundException(String message) {
    super(message);
  }
}
