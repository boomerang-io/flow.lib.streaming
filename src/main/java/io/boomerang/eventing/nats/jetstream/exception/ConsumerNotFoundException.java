package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link ConsumerNotFoundException} is used to indicate that NATS Jetstream
 * {@code Consumer} could not be found on the server.
 * 
 * @since 0.1.0
 */
public class ConsumerNotFoundException extends RuntimeException {

  public ConsumerNotFoundException(String message) {
    super(message);
  }
}
