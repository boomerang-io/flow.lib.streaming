package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link StreamNotFoundException} is used to indicate that NATS Jetstream
 * {@code Stream} could not be found on the server.
 * 
 * @since 0.1.0
 */
public class StreamNotFoundException extends RuntimeException {

  public StreamNotFoundException(String message) {
    super(message);
  }
}
