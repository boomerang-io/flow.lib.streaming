package io.boomerang.eventing.nats.jetstream;

import io.boomerang.eventing.nats.jetstream.exception.AlreadySubscribedException;
import io.boomerang.eventing.nats.jetstream.exception.ConsumerNotFoundException;
import io.boomerang.eventing.nats.jetstream.exception.StreamNotFoundException;

public interface SubOnlyTunnel {

  /**
   * Subscribe to receive new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @param handler Subscription handler. This handler is assigned as a weak reference, thus the
   *        persistence of the handler is up to you.
   * @throws AlreadySubscribedException Thrown when this communication tunnel is already subscribed
   *         to receive new messages.
   * @throws StreamNotFoundException Thrown when the provided {@code Stream} was not found on the
   *         server and {@link PubSubConfiguration#isAutomaticallyCreateStream} is set to
   *         {@code false}. Consider enabling
   *         {@link PubSubConfiguration#isAutomaticallyCreateStream} to automatically create a
   *         {@code Stream} on the NATS server.
   * @throws ConsumerNotFoundException Thrown when the provided {@code Consumer} was not found on
   *         the server and {@link PubSubConfiguration#isAutomaticallyCreateConsumer} is set to
   *         {@code false}. Consider enabling
   *         {@link PubSubConfiguration#isAutomaticallyCreateConsumer} to automatically create a
   *         {@code Stream} on the NATS server.
   * @since 0.2.0
   */
  public void subscribe(SubHandler handler)
      throws AlreadySubscribedException, StreamNotFoundException, ConsumerNotFoundException;

  /**
   * Unsubscribe from receiving new messages from the NATS Jetstream {@code Consumer}.
   * 
   * @since 0.2.0
   * @note Does nothing if this communication tunnel is not subscribed.
   */
  public void unsubscribe();

  /**
   * @return {@code true} if this communication tunnel is subscribed for receiving new messages from
   *         the NATS Jetstream {@code Consumer}, {@code false} otherwise.
   * @since 0.2.0
   * @see also For additional details on active subscription, see {@link #isSubscriptionActive()}
   *      property .
   * @note If {@code true}, this doesn't mean necessarily that the subscription is currently active,
   *       i. e. listening for new messages. It means that the handler for subscription has been
   *       assigned. Once the subscription becomes active, the {@link #isSubscriptionActive()} will
   *       also return {@code true}.
   */
  public Boolean isSubscribed();

  /**
   * @return {@code true} if this communication tunnel is <b>subscribed and listening</b> for new
   *         messages from the NATS Jetstream {@code Consumer}, {@code false} otherwise.
   * @since 0.2.0
   * @note The subscription will remain active for push-based {@code Consumers}, since these are
   *       build to stay active even if the connection to the NATS server is lost and then
   *       re-established. On the other hand, if a connection to the NATS server is lost, a
   *       subscription to a pull-based {@code Consumers} will not be active, thus
   *       {@link #isSubscriptionActive()} will return {@code false}, until the connection is
   *       re-established.
   */
  public Boolean isSubscriptionActive();
}
