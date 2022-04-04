package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link MisconfigurationException} is used to indicate that a certain configuration object was
 * misconfigured, does not contain required data or contains extraneous data.
 * 
 * @since 0.3.0
 */
public class MisconfigurationException extends RuntimeException {

  public MisconfigurationException(String message) {
    super(message);
  }
}
