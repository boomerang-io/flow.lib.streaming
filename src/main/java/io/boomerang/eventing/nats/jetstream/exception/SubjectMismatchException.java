package io.boomerang.eventing.nats.jetstream.exception;

/**
 * {@link SubjectMismatchException} is used to indicate that message's subject
 * does not match {@code Stream}'s subject. Note that the subject of a
 * {@code Stream can also be a wildcard.
 * 
 * @see https://docs.nats.io/nats-concepts/subjects
 * 
 * @since 0.1.0
 */
public class SubjectMismatchException extends RuntimeException {

  public SubjectMismatchException(String message) {
    super(message);
  }
}
