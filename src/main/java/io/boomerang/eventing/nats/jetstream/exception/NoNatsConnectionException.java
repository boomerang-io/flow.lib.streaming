package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link NoNatsConnectionException} is used to indicate that there is no active
 * connection to the NATS Jetstream server.
 * 
 * @since 0.1.0
 */
public class NoNatsConnectionException extends RuntimeException {

  public NoNatsConnectionException(String message) {
    super(message);
  }
}
