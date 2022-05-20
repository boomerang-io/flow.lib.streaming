package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
import io.boomerang.eventing.nats.jetstream.exception.FailedPublishMessageException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.SubjectMismatchException;
import io.nats.client.JetStreamApiException;

/**
 * Publish-only communication tunnel interface. Can only publish new messages to a NATS Jetstream
 * {@code Stream}.
 * 
 * @since 0.2.0
 */
public interface PubOnlyTunnel {

  /**
   * Publish a new message to the NATS Jetstream {@code Stream}.
   * 
   * @param subject The subject of the message. Must match the {@code Stream}'s subject.
   * @param message The message itself.
   * @throws IOException NATS server communication error.
   * @throws JetStreamApiException NATS Jetstream related error (e. g. Jetstream context is not
   *         enabled on this server).
   * @throws StreamNotFoundException Could not find the {@code Stream} to publish message to.
   * @throws SubjectMismatchException Message's subject does not match {@code Stream}'s subject (can
   *         be a wildcard).
   * @throws FailedPublishMessageException The message failed to publish. This could due to a
   *         connection issue or Jetstream issue.
   * @since 0.2.0
   */
  public void publish(String subject, String message) throws IOException, JetStreamApiException,
      StreamNotFoundException, SubjectMismatchException, FailedPublishMessageException;

  /**
   * Publish a new message to the NATS Jetstream {@code Stream} with the option to retry if the
   * initial publish fails.
   * 
   * @param subject The subject of the message. Must match the {@code Stream}'s subject.
   * @param message The message itself.
   * @param republishOnFail If publishing of the message fails due to connection issues, enabling
   *        this flag will allow for storing the failed message and attempting to republish once
   *        connection is reestablished.
   * @throws IOException NATS server communication error.
   * @throws JetStreamApiException NATS Jetstream related error (e. g. Jetstream context is not
   *         enabled on this server).
   * @throws StreamNotFoundException Could not find the {@code Stream} to publish message to.
   * @throws SubjectMismatchException Message's subject does not match {@code Stream}'s subject (can
   *         be a wildcard).
   * @throws FailedPublishMessageException The message failed to publish. This could due to a
   *         connection issue or Jetstream issue.
   * @since 0.4.0
   */
  public void publish(String subject, String message, Boolean republishOnFail)
      throws IOException, JetStreamApiException, StreamNotFoundException, SubjectMismatchException,
      FailedPublishMessageException;
}
