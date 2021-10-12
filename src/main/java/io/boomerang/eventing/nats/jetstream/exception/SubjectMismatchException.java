package io.boomerang.eventing.nats.jetstream.exception;

public class SubjectMismatchException extends RuntimeException {

  public SubjectMismatchException(String message) {
    super(message);
  }
}
