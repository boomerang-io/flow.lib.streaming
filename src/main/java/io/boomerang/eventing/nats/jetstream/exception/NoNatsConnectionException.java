package io.boomerang.eventing.nats.jetstream.exception;

public class NoNatsConnectionException extends RuntimeException {

  public NoNatsConnectionException(String message) {
    super(message);
  }
}
