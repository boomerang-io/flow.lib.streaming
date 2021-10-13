package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link AlreadySubscribedException} is used to indicate that a communication tunnel
 * is already subscribed for receiving new messages.
 * 
 * @since 0.2.0
 */
public class AlreadySubscribedException extends RuntimeException {

  public AlreadySubscribedException(String message) {
    super(message);
  }
}
