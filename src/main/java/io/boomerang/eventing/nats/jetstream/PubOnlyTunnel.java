package io.boomerang.eventing.nats.jetstream;

import java.io.IOException;
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
   * @since 0.2.0
   */
  public void publish(String subject, String message)
      throws IOException, JetStreamApiException, StreamNotFoundException, SubjectMismatchException;
}
