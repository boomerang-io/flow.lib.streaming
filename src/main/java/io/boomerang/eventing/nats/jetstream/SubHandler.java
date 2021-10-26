package io.boomerang.eventing.nats.jetstream;

/**
 * Message subscription handler for subscribing and receiving new messages from a NATS Jetstream
 * {@code Consumer}.
 * 
 * @since 0.2.0
 */
public interface SubHandler {

  /**
   * This method is invoked when the {@link PubSubTunnel PubSubTunnel} communication tunnel has
   * succeeded to subscribe for receiving new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param tunnel The communication tunnel that has succeeded to subscribe for receiving new
   *        messages.
   * @since 0.2.0
   * @note The default implementation of this method does nothing.
   */
  public default void subscriptionSucceeded(PubSubTunnel tunnel) {}

  /**
   * This method is invoked when the {@link PubSubTunnel PubSubTunnel} communication tunnel has
   * failed to subscribe for receiving new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param tunnel The communication tunnel that has failed to subscribe for receiving new messages.
   * @param exception The exception error.
   * @since 0.2.0
   * @note The default implementation of this method does nothing.
   * @note The communication tunnel is automatically unsubscribed when this method is invoked.
   */
  public default void subscriptionFailed(PubSubTunnel tunnel, Exception exception) {}

  /**
   * This method is invoked when a new message was received from the NATS Jetstream
   * {@code Consumer}.
   * 
   * @param tunnel The communication tunnel that has received the message.
   * @param subject The subject of the message.
   * @param message The message itself.
   * @since 0.2.0
   */
  public void newMessageReceived(PubSubTunnel tunnel, String subject, String message);
}
