package io.boomerang.eventing.nats.jetstream;

/**
 * Message subscription handler for subscribing and receiving new messages from a NATS Jetstream
 * {@code Consumer}.
 * 
 * @since 0.3.0
 */
public interface SubHandler {

  /**
   * This method is invoked when the {@link SubOnlyTunnel SubOnlyTunnel} communication tunnel has
   * succeeded to subscribe for receiving new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param tunnel The communication tunnel that has succeeded to subscribe for receiving new
   *        messages.
   * @since 0.3.0
   * @note The default implementation of this method does nothing.
   * @note This method might be invoked multiple times if the connection to the NATS server drops
   *       and then is re-established, however it also might be never invoked if a connection to the
   *       NATS server was never established in the first place.
   */
  public default void subscriptionSucceeded(SubOnlyTunnel tunnel) {}

  /**
   * This method is invoked when the {@link SubOnlyTunnel SubOnlyTunnel} communication tunnel has
   * failed to subscribe for receiving new messages from the NATS Jetstream {@code Consumer}. By
   * nature, this means there was an error when communicating to the Jetstream {@code Consumer} and
   * it is not related to the connection to the NATS server itself.
   * 
   * @param tunnel The communication tunnel that has failed to subscribe for receiving new messages.
   * @param exception The exception error.
   * @since 0.3.0
   * @note The default implementation of this method does nothing.
   * @note This method will be invoked at most once after a subscription was request. Once invoked,
   *       the communication tunnel will be automatically unsubscribed.
   */
  public default void subscriptionFailed(SubOnlyTunnel tunnel, Exception exception) {}

  /**
   * This method is invoked when a new message was received from the NATS Jetstream
   * {@code Consumer}.
   * 
   * @param tunnel The communication tunnel that has received the message.
   * @param subject The subject of the message.
   * @param message The message itself.
   * @since 0.3.0
   */
  public void newMessageReceived(SubOnlyTunnel tunnel, String subject, String message);
}
