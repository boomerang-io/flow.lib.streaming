package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link FailedPublishMessageException} is used to indicate that publishing a NATS message has
 * failed.
 * 
 * @since 0.4.0
 */
public class FailedPublishMessageException extends RuntimeException {

  public FailedPublishMessageException(String message) {
    super(message);
  }

  public FailedPublishMessageException(String message, Exception cause) {
    super(message, cause);
  }
}
