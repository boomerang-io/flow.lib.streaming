package io.boomerang.eventing.nats.jetstream;

import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;

/**
 * Publish and subscribe communication tunnel interface. Can both publish and
 * subscribe for sending and receiving new messages from a NATS Jetstream
 * {@code Stream} and {@code Consumer}.
 * 
 * @since 0.2.0
 */
public interface PubSubTunnel extends PubOnlyTunnel {

  /**
   * Subscribe to receive new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param handler Subscription handler.
   * @throws AlreadySubscribedException Thrown when this communication tunnel is
   *                                    already subscribed to receive new
   *                                    messages.
   * @since 0.2.0
   */
  public void subscribe(SubHandler handler) throws AlreadySubscribedException;

  /**
   * Unsubscribe from receiving new messages from the NATS Jetstream
   * {@code Consumer}.
   * 
   * @since 0.2.0
   * @note Does nothing if this communication tunnel is not subscribed.
   */
  public void unsubscribe();

  /**
   * @return {@code true} if this communication tunnel is subscribed for receiving
   *         new messages from the NATS Jetstream {@code Consumer}, {@code false}
   *         otherwise.
   * @since 0.2.0
   * @see also For additional details on active subscription, see
   *      {@link #isSubscriptionActive()} property .
   * @note If {@code true}, this doesn't mean necessarily that the subscription is
   *       currently active, i. e. listening for new messages. It means that the
   *       handler for subscription has been assigned and once active, the
   *       {@link #isSubscriptionActive()} will be set to {@code true}.
   */
  public Boolean isSubscribed();

  /**
   * @return {@code true} if this communication tunnel is <b>subscribed and
   *         listening</b> for new messages from the NATS Jetstream
   *         {@code Consumer}, {@code false} otherwise.
   * @since 0.2.0
   */
  public Boolean isSubscriptionActive();
}
